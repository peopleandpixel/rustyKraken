#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for S3/MinIO Reflex
/// Uploads objects to S3-compatible storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3MinioReflexConfig {
    /// S3/MinIO endpoint URL (e.g., "http://localhost:9000")
    pub endpoint: String,

    /// AWS Access Key ID
    pub access_key: String,

    /// AWS Secret Access Key
    pub secret_key: String,

    /// Default bucket name
    #[serde(default = "default_bucket")]
    pub bucket: String,

    /// Template for bucket name
    #[serde(default)]
    pub bucket_template: Option<String>,

    /// Template for object key/path
    #[serde(default = "default_key_template")]
    pub key_template: String,

    /// Template for object body
    #[serde(default = "default_body_template")]
    pub body_template: String,

    /// Content type
    #[serde(default = "default_content_type")]
    pub content_type: String,

    /// Optional: mapping from Ink topics to buckets
    #[serde(default)]
    pub topic_bucket_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to key templates
    #[serde(default)]
    pub topic_key_mapping: HashMap<String, String>,

    /// Use HTTPS
    #[serde(default)]
    pub use_https: bool,

    /// Region (for AWS S3)
    #[serde(default)]
    pub region: Option<String>,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_bucket() -> String {
    "kraken".to_string()
}
fn default_key_template() -> String {
    "{{topic}}/{{id}}".to_string()
}
fn default_body_template() -> String {
    "{{body}}".to_string()
}
fn default_content_type() -> String {
    "text/plain".to_string()
}
fn default_verify_ssl() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}

pub struct S3MinioReflex {
    name: String,
    config: S3MinioReflexConfig,
    client: reqwest::Client,
}

impl S3MinioReflex {
    pub fn new(name: String, config: S3MinioReflexConfig) -> Self {
        let verify_ssl = config.verify_ssl;
        Self {
            name,
            config,
            client: reqwest::Client::builder()
                .danger_accept_invalid_certs(!verify_ssl)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// Resolve bucket name for a given Ink
    fn resolve_bucket(&self, ink: &Ink) -> String {
        if let Some(bucket) = self
            .config
            .topic_bucket_mapping
            .get(&ink.topic)
            .filter(|b| !b.is_empty())
        {
            return bucket.clone();
        }
        if let Some(ref template) = self.config.bucket_template {
            return self.render_template(template, ink);
        }
        self.config.bucket.clone()
    }

    /// Resolve object key for a given Ink
    fn resolve_key(&self, ink: &Ink) -> String {
        if let Some(key) = self
            .config
            .topic_key_mapping
            .get(&ink.topic)
            .filter(|k| !k.is_empty())
        {
            return self.render_template(key, ink);
        }
        self.render_template(&self.config.key_template, ink)
    }

    fn render_template(&self, template: &str, ink: &Ink) -> String {
        let mut rendered = template.to_string();
        rendered = rendered.replace("{{topic}}", &ink.topic);
        rendered = rendered.replace("{{body}}", &ink.body);
        rendered = rendered.replace("{{id}}", &ink.id.to_string());
        rendered = rendered.replace("{{timestamp}}", &ink.timestamp.to_rfc3339());
        if let Some(obj) = ink.metadata.as_ref().and_then(|m| m.as_object()) {
            for (key, value) in obj {
                let placeholder = format!("{{{{metadata.{}}}}}", key);
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                rendered = rendered.replace(&placeholder, &value_str);
            }
        }
        rendered
    }

    /// Build S3 endpoint URL
    fn s3_url(&self, bucket: &str, key: &str) -> String {
        let scheme = if self.config.use_https { "https" } else { "http" };
        let endpoint = self.config.endpoint.trim_end_matches('/');

        // Check if endpoint already includes scheme
        if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            let host = if self.config.use_https {
                endpoint.strip_prefix("http://").unwrap_or(endpoint)
            } else {
                endpoint.strip_prefix("https://").unwrap_or(endpoint)
            };
            format!("{}/{}{}", scheme, host, self.object_path(bucket, key))
        } else {
            format!("{}/{}{}", scheme, endpoint, self.object_path(bucket, key))
        }
    }

    fn object_path(&self, bucket: &str, key: &str) -> String {
        format!("/{}/{}", bucket, key)
    }

    /// Simple HMAC-SHA256 signature for S3 (v4)
    fn sign_request(
        &self,
        method: &str,
        url: &str,
        body: &str,
        content_type: &str,
        date: &str,
        date_stamp: &str,
    ) -> (String, Vec<String>) {
        use sha2::{Digest, Sha256};

        let region = self.config.region.as_deref().unwrap_or("us-east-1");
        let service = "s3";

        // Canonical Request
        let canonical_uri = "/";
        let canonical_headers = format!(
            "content-type:{}\nhost:{}\nx-amz-date:{}\n",
            content_type,
            url.split("//").nth(1).unwrap_or(""),
            date
        );
        let signed_headers = "content-type;host;x-amz-date";

        let payload_hash = {
            let mut hasher = Sha256::new();
            hasher.update(body.as_bytes());
            hex::encode(hasher.finalize())
        };

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method, canonical_uri, "", canonical_headers, signed_headers, payload_hash
        );

        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}/{}/{}/aws4_request\n{}",
            date,
            date_stamp,
            region,
            service,
            {
                let mut hasher = Sha256::new();
                hasher.update(canonical_request.as_bytes());
                hex::encode(hasher.finalize())
            }
        );

        // Signing key
        let signing_key = {
            let key = format!("AWS4{}", self.config.secret_key).into_bytes();
            let date_key = hmac_sha256(&key, date_stamp.as_bytes());
            let region_key = hmac_sha256(&date_key, region.as_bytes());
            let service_key = hmac_sha256(&region_key, service.as_bytes());
            hmac_sha256(&service_key, b"aws4_request")
        };

        let signature = {
            let signed = hmac_sha256(&signing_key, string_to_sign.as_bytes());
            hex::encode(&signed)
        };

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}/{}/{}/aws4_request, SignedHeaders={}, Signature={}",
            self.config.access_key, date_stamp, region, service, signed_headers, signature
        );

        let headers = vec![
            format!("x-amz-date: {}", date),
            format!("x-amz-content-sha256: {}", payload_hash),
            authorization,
        ];

        (payload_hash, headers)
    }

    /// Upload an object to S3
    async fn put_object(&self, bucket: &str, key: &str, body: &str) -> Result<()> {
        let url = self.s3_url(bucket, key);

        let date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = chrono::Utc::now().format("%Y%m%d").to_string();

        let (_payload_hash, headers) = self.sign_request("PUT", &url, body, &self.config.content_type, &date, &date_stamp);

        let mut request = self
            .client
            .put(&url)
            .header("Content-Type", &self.config.content_type);

        for header in &headers {
            if let Some(auth) = header.strip_prefix("Authorization: ") {
                request = request.header("Authorization", auth);
            } else if let Some(date_h) = header.strip_prefix("x-amz-date: ") {
                request = request.header("x-amz-date", date_h);
            } else if let Some(sha) = header.strip_prefix("x-amz-content-sha256: ") {
                request = request.header("x-amz-content-sha256", sha);
            }
        }

        let response = request
            .body(body.to_string())
            .send()
            .await
            .with_context(|| format!("Failed to upload object to S3 at {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "S3 upload error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }

    /// List objects in a bucket
    async fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let url = format!(
            "{}/{}?list-type=2&prefix={}",
            self.config.endpoint.trim_end_matches('/'),
            bucket,
            urlencoding::encode(prefix)
        );

        let response = self.client.get(&url).send().await
            .with_context(|| format!("Failed to list objects in S3 bucket {}", bucket))?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("S3 list objects error: {}", status));
        }

        // Parse XML response (simplified)
        let body = response.text().await?;
        let mut objects = Vec::new();
        for line in body.lines() {
            if line.contains("<Key>")
                && let Some(key) = line.strip_prefix("<Key>").and_then(|s| s.strip_suffix("</Key>")) {
                    objects.push(key.to_string());
                }
        }

        Ok(objects)
    }

    /// Delete an object
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        let url = self.s3_url(bucket, key);

        let response = self.client.delete(&url).send().await
            .with_context(|| "Failed to delete object from S3".to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "S3 delete error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }
}

/// HMAC-SHA256 helper
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<sha2::Sha256>;

    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

#[async_trait]
impl Reflex for S3MinioReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "s3_minio"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "S3/MinIO reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        // Resolve bucket
        let bucket = self.resolve_bucket(ink);
        debug!(
            "S3/MinIO reflex '{}' resolved bucket: {}",
            self.name, bucket
        );

        // Resolve key
        let key = self.resolve_key(ink);
        debug!(
            "S3/MinIO reflex '{}' resolved key: {}",
            self.name, key
        );

        // Render body
        let body = self.render_template(&self.config.body_template, ink);
        debug!(
            "S3/MinIO reflex '{}' body length: {}",
            self.name, body.len()
        );

        // Upload object
        self.put_object(&bucket, &key, &body).await?;

        info!(
            "S3/MinIO reflex '{}' uploaded object {}/{}",
            self.name, bucket, key
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> S3MinioReflexConfig {
        S3MinioReflexConfig {
            endpoint: "http://localhost:9000".to_string(),
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
            bucket: "kraken".to_string(),
            bucket_template: None,
            key_template: "{{topic}}/{{id}}".to_string(),
            body_template: "{{body}}".to_string(),
            content_type: "text/plain".to_string(),
            topic_bucket_mapping: HashMap::new(),
            topic_key_mapping: HashMap::new(),
            use_https: false,
            region: None,
            verify_ssl: true,
            timeout: 30,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = S3MinioReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("{{topic}}/{{id}}", &ink),
            format!("/test/topic/{}", ink.id)
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            endpoint: "http://minio:9000"
            access_key: "mykey"
            secret_key: "mysecret"
            bucket: "mybucket"
        "#;
        let config: S3MinioReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.endpoint, "http://minio:9000");
        assert_eq!(config.bucket, "mybucket");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = S3MinioReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "s3_minio");
    }

    #[test]
    fn test_resolve_bucket_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/logs/app".to_string(), "app-logs".to_string());
        config.topic_bucket_mapping = mapping;

        let reflex = S3MinioReflex::new("test".to_string(), config);
        let ink = Ink::new("/logs/app", "test", "test_source", "body");
        assert_eq!(reflex.resolve_bucket(&ink), "app-logs");
    }
}
