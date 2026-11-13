//! Application context holding shared resources like the HTTP client pool.
//!
//! Instead of each Reflex/Tentacle creating its own `reqwest::Client`,
//! we create a single pooled client at startup and share it via `Arc`.

use reqwest::Client;
use std::sync::Arc;

/// Shared application context containing pooled resources.
#[derive(Clone)]
pub struct AppContext {
    /// The shared HTTP client with connection pooling enabled.
    pub http_client: Arc<Client>,
}

impl AppContext {
    /// Create a new `AppContext` with a pooled HTTP client.
    ///
    /// The client is configured with:
    /// - Connection pooling (enabled by default in reqwest)
    /// - HTTP/2 support
    /// - Reasonable timeouts
    pub fn new() -> Self {
        let client = Client::builder()
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Some(std::time::Duration::from_secs(90)))
            .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
            .build()
            .expect("Failed to build default HTTP client");

        Self {
            http_client: Arc::new(client),
        }
    }

    /// Get a reference to the shared HTTP client.
    pub fn http_client(&self) -> &Arc<Client> {
        &self.http_client
    }

    /// Create a custom HTTP client that accepts invalid certificates.
    /// Used by Reflexes/Tentacles that need to connect to servers with self-signed certs.
    pub fn http_client_danger_accept_invalid_certs(&self) -> Client {
        Client::builder()
            .danger_accept_invalid_certs(true)
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Some(std::time::Duration::from_secs(90)))
            .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
            .build()
            .expect("Failed to build HTTP client with invalid certs")
    }

    /// Create a custom HTTP client with redirect configuration.
    #[allow(dead_code)]
    pub fn http_client_with_redirect(&self, follow_redirects: bool) -> Client {
        Client::builder()
            .redirect(if follow_redirects {
                reqwest::redirect::Policy::default()
            } else {
                reqwest::redirect::Policy::none()
            })
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Some(std::time::Duration::from_secs(90)))
            .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
            .build()
            .expect("Failed to build HTTP client with redirect config")
    }

    /// Create a custom HTTP client with both verify_ssl and redirect options.
    pub fn http_client_custom(&self, verify_ssl: bool, follow_redirects: bool) -> Client {
        let builder = Client::builder()
            .danger_accept_invalid_certs(!verify_ssl)
            .redirect(if follow_redirects {
                reqwest::redirect::Policy::default()
            } else {
                reqwest::redirect::Policy::none()
            })
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Some(std::time::Duration::from_secs(90)))
            .tcp_keepalive(Some(std::time::Duration::from_secs(60)));

        builder.build().expect("Failed to build custom HTTP client")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_context_creation() {
        let ctx = AppContext::new();
        // Client should be created successfully
        assert!(Arc::strong_count(&ctx.http_client) == 1);
    }

    #[test]
    fn test_app_context_clone() {
        let ctx = AppContext::new();
        let ctx2 = ctx.clone();
        assert!(Arc::ptr_eq(&ctx.http_client, &ctx2.http_client));
    }

    #[test]
    fn test_http_client_danger_accept_invalid_certs() {
        let ctx = AppContext::new();
        let _client = ctx.http_client_danger_accept_invalid_certs();
        // Should not panic
    }

    #[test]
    fn test_http_client_with_redirect() {
        let ctx = AppContext::new();
        let _client_follow = ctx.http_client_with_redirect(true);
        let _client_no_follow = ctx.http_client_with_redirect(false);
        // Should not panic
    }

    #[test]
    fn test_http_client_custom() {
        let ctx = AppContext::new();
        let _client1 = ctx.http_client_custom(true, true);
        let _client2 = ctx.http_client_custom(false, false);
        // Should not panic
    }
}
