use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Modbus-Funktion zum Lesen
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ModbusFunction {
    /// Read Coils (0x01)
    ReadCoils,
    /// Read Discrete Inputs (0x02)
    ReadDiscreteInputs,
    /// Read Holding Registers (0x03)
    ReadHoldingRegisters,
    /// Read Input Registers (0x04)
    ReadInputRegisters,
    /// Write Single Coil (0x05)
    WriteSingleCoil,
    /// Write Single Register (0x06)
    WriteSingleRegister,
    /// Write Multiple Coils (0x0F)
    WriteMultipleCoils,
    /// Write Multiple Registers (0x10)
    WriteMultipleRegisters,
}

impl ModbusFunction {
    fn code(&self) -> u8 {
        match self {
            ModbusFunction::ReadCoils => 0x01,
            ModbusFunction::ReadDiscreteInputs => 0x02,
            ModbusFunction::ReadHoldingRegisters => 0x03,
            ModbusFunction::ReadInputRegisters => 0x04,
            ModbusFunction::WriteSingleCoil => 0x05,
            ModbusFunction::WriteSingleRegister => 0x06,
            ModbusFunction::WriteMultipleCoils => 0x0F,
            ModbusFunction::WriteMultipleRegisters => 0x10,
        }
    }
}

/// Verbindungstyp
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ModbusConnection {
    #[default]
    Tcp,
    Serial,
}

/// Konfiguration für ein Modbus-Register/Coil zu überwachen
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModbusPoint {
    /// Name des Punkts (z.B. "temperature", "pressure")
    pub name: String,
    /// Modbus-Funktion
    #[serde(default = "default_function")]
    pub function: ModbusFunction,
    /// Start-Adresse (0-basiert)
    pub address: u16,
    /// Anzahl der Register/Coils zu lesen
    #[serde(default = "default_quantity")]
    pub quantity: u16,
    /// Slave ID
    #[serde(default = "default_slave_id")]
    pub slave_id: u8,
    /// Topic-Präfix für diesen Punkt
    pub topic: Option<String>,
}

fn default_function() -> ModbusFunction {
    ModbusFunction::ReadHoldingRegisters
}

fn default_quantity() -> u16 {
    1
}

fn default_slave_id() -> u8 {
    1
}

/// Konfiguration für den Modbus Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModbusTentacleConfig {
    /// Verbindungstyp
    #[serde(default)]
    pub connection: ModbusConnection,
    /// Server-Adresse (IP für TCP, /dev/ttyUSB0 für Serial)
    pub host: String,
    /// Port (für TCP, default 502)
    #[serde(default = "default_port")]
    pub port: u16,
    /// Baudrate (für Serial)
    #[serde(default = "default_baudrate")]
    pub baudrate: u32,
    /// Polling-Interval in Millisekunden
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Timeout in Millisekunden
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Reconnect-Verzögerung in Sekunden
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_secs: u64,
    /// Globales Topic
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Zu überwachende Punkte
    pub points: Vec<ModbusPoint>,
    /// Transaktions-ID Startwert
    #[serde(default = "default_transaction_id")]
    pub transaction_id_start: u16,
}

fn default_port() -> u16 {
    502
}

fn default_baudrate() -> u32 {
    9600
}

fn default_poll_interval_ms() -> u64 {
    1000
}

fn default_timeout_ms() -> u64 {
    1000
}

fn default_reconnect_delay() -> u64 {
    5
}

fn default_topic() -> String {
    "/modbus/data".to_string()
}

fn default_transaction_id() -> u16 {
    1
}

/// Modbus TCP Frame Builder
struct ModbusTcpFrame {
    transaction_id: u16,
    protocol_id: u16,
    length: u16,
    unit_id: u8,
    function_code: u8,
    data: Vec<u8>,
}

impl ModbusTcpFrame {
    fn new(transaction_id: u16, unit_id: u8, function_code: u8, data: Vec<u8>) -> Self {
        let length = (1 + data.len()) as u16;
        Self {
            transaction_id,
            protocol_id: 0,
            length,
            unit_id,
            function_code,
            data,
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let mut frame = Vec::with_capacity(12 + self.data.len());
        frame.extend_from_slice(&self.transaction_id.to_be_bytes());
        frame.extend_from_slice(&self.protocol_id.to_be_bytes());
        frame.extend_from_slice(&self.length.to_be_bytes());
        frame.push(self.unit_id);
        frame.push(self.function_code);
        frame.extend_from_slice(&self.data);
        frame
    }

    fn read_holding_registers(
        transaction_id: u16,
        slave_id: u8,
        address: u16,
        quantity: u16,
    ) -> Self {
        let data = vec![
            (address >> 8) as u8,
            (address & 0xFF) as u8,
            (quantity >> 8) as u8,
            (quantity & 0xFF) as u8,
        ];
        Self::new(transaction_id, slave_id, 0x03, data)
    }

    fn read_input_registers(
        transaction_id: u16,
        slave_id: u8,
        address: u16,
        quantity: u16,
    ) -> Self {
        let data = vec![
            (address >> 8) as u8,
            (address & 0xFF) as u8,
            (quantity >> 8) as u8,
            (quantity & 0xFF) as u8,
        ];
        Self::new(transaction_id, slave_id, 0x04, data)
    }

    fn read_coils(transaction_id: u16, slave_id: u8, address: u16, quantity: u16) -> Self {
        let data = vec![
            (address >> 8) as u8,
            (address & 0xFF) as u8,
            (quantity >> 8) as u8,
            (quantity & 0xFF) as u8,
        ];
        Self::new(transaction_id, slave_id, 0x01, data)
    }

    fn read_discrete_inputs(
        transaction_id: u16,
        slave_id: u8,
        address: u16,
        quantity: u16,
    ) -> Self {
        let data = vec![
            (address >> 8) as u8,
            (address & 0xFF) as u8,
            (quantity >> 8) as u8,
            (quantity & 0xFF) as u8,
        ];
        Self::new(transaction_id, slave_id, 0x02, data)
    }
}

/// Modbus TCP Response Parser
struct ModbusTcpResponse {
    #[allow(dead_code)]
    transaction_id: u16,
    #[allow(dead_code)]
    function_code: u8,
    #[allow(dead_code)]
    byte_count: u8,
    data: Vec<u8>,
    is_error: bool,
    error_code: u8,
}

impl ModbusTcpResponse {
    fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 8 {
            return None;
        }
        let transaction_id = u16::from_be_bytes([bytes[0], bytes[1]]);
        // protocol_id = bytes[2..4] (should be 0)
        let _length = u16::from_be_bytes([bytes[4], bytes[5]]);
        let _unit_id = bytes[6];
        let function_code = bytes[7];

        let (is_error, error_code, byte_count, data) = if function_code & 0x80 != 0 {
            // Error response
            let error_code = if bytes.len() > 8 { bytes[8] } else { 0 };
            (true, error_code, 0, Vec::new())
        } else {
            if bytes.len() < 9 {
                return None;
            }
            let byte_count = bytes[8];
            let expected_len = 9 + byte_count as usize;
            if bytes.len() < expected_len {
                return None;
            }
            let data = bytes[9..expected_len].to_vec();
            (false, 0, byte_count, data)
        };

        Some(Self {
            transaction_id,
            function_code: function_code & 0x7F,
            byte_count,
            data,
            is_error,
            error_code,
        })
    }

    /// Parse register values as u16
    fn register_values(&self) -> Vec<u16> {
        let mut values = Vec::new();
        let mut i = 0;
        while i + 1 < self.data.len() {
            values.push(u16::from_be_bytes([self.data[i], self.data[i + 1]]));
            i += 2;
        }
        values
    }

    /// Parse coil values as bool
    fn coil_values(&self, quantity: u16) -> Vec<bool> {
        let mut values = Vec::new();
        for byte in &self.data {
            for bit in 0..8 {
                if values.len() >= quantity as usize {
                    break;
                }
                values.push((byte >> bit) & 1 == 1);
            }
        }
        values
    }
}

pub struct ModbusTentacle {
    name: String,
    config: ModbusTentacleConfig,
    transaction_id: Arc<RwLock<u16>>,
    last_values: Arc<RwLock<HashMap<String, serde_json::Value>>>,
}

impl ModbusTentacle {
    pub fn new(name: String, config: ModbusTentacleConfig) -> Self {
        Self {
            name,
            config,
            transaction_id: Arc::new(RwLock::new(0)),
            last_values: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn next_transaction_id(&self) -> u16 {
        let mut id = self.transaction_id.write().unwrap();
        *id = id.wrapping_add(1);
        if *id == 0 {
            *id = 1;
        }
        *id
    }

    fn get_point_topic(&self, point: &ModbusPoint) -> String {
        point
            .topic
            .clone()
            .unwrap_or_else(|| format!("{}/{}", self.config.topic, point.name))
    }

    /// Build a Modbus Ink message from point data
    fn create_ink(&self, point: &ModbusPoint, values: &serde_json::Value) -> Ink {
        let topic = self.get_point_topic(point);
        let mut meta_map = serde_json::Map::new();
        meta_map.insert("point_name".into(), serde_json::json!(&point.name));
        meta_map.insert("function".into(), serde_json::json!(point.function.code()));
        meta_map.insert("address".into(), serde_json::json!(point.address));
        meta_map.insert("slave_id".into(), serde_json::json!(point.slave_id));
        meta_map.insert("host".into(), serde_json::json!(&self.config.host));
        meta_map.insert("values".into(), values.clone());

        Ink::new(
            &topic,
            "modbus",
            &self.name,
            format!("Modbus {}: {:?}", point.name, values),
        )
        .with_metadata(serde_json::Value::Object(meta_map))
    }

    /// Simulate reading a Modbus point (for testing without real hardware)
    async fn read_point(&self, point: &ModbusPoint) -> Result<serde_json::Value> {
        let tx_id = self.next_transaction_id();
        let frame = match point.function {
            ModbusFunction::ReadHoldingRegisters => ModbusTcpFrame::read_holding_registers(
                tx_id,
                point.slave_id,
                point.address,
                point.quantity,
            ),
            ModbusFunction::ReadInputRegisters => ModbusTcpFrame::read_input_registers(
                tx_id,
                point.slave_id,
                point.address,
                point.quantity,
            ),
            ModbusFunction::ReadCoils => {
                ModbusTcpFrame::read_coils(tx_id, point.slave_id, point.address, point.quantity)
            }
            ModbusFunction::ReadDiscreteInputs => ModbusTcpFrame::read_discrete_inputs(
                tx_id,
                point.slave_id,
                point.address,
                point.quantity,
            ),
            _ => {
                warn!(
                    "Modbus Tentacle '{}': write function {:?} not supported for polling",
                    self.name, point.function
                );
                return Ok(serde_json::json!(null));
            }
        };

        let payload = frame.serialize();
        debug!(
            "Modbus Tentacle '{}': reading {} (addr={}, qty={}) with tx_id={}",
            self.name, point.name, point.address, point.quantity, tx_id
        );

        // Try TCP connection
        if self.config.connection == ModbusConnection::Tcp {
            match self.read_tcp(&payload, point).await {
                Ok(values) => return Ok(values),
                Err(e) => {
                    warn!(
                        "Modbus Tentacle '{}': TCP read failed for {}: {}",
                        self.name, point.name, e
                    );
                }
            }
        }

        // Fallback: simulate values for testing
        warn!(
            "Modbus Tentacle '{}': no connection, simulating values for {}",
            self.name, point.name
        );
        self.simulate_values(point)
    }

    async fn read_tcp(&self, payload: &[u8], point: &ModbusPoint) -> Result<serde_json::Value> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let mut stream = tokio::time::timeout(
            Duration::from_millis(self.config.timeout_ms),
            tokio::net::TcpStream::connect(&addr),
        )
        .await??;

        use tokio::io::AsyncWriteExt;
        tokio::time::timeout(
            Duration::from_millis(self.config.timeout_ms),
            stream.write_all(payload),
        )
        .await??;

        let mut buf = Vec::with_capacity(256);
        let mut byte = [0u8; 1];
        // Read MBAP header (7 bytes) + at least 2 bytes response
        for _ in 0..9 {
            match tokio::time::timeout(
                Duration::from_millis(self.config.timeout_ms),
                tokio::io::AsyncReadExt::read_exact(&mut stream, &mut byte),
            )
            .await
            {
                Ok(Ok(_)) => buf.push(byte[0]),
                _ => return Err(anyhow::anyhow!("Timeout reading response")),
            }
        }

        // Parse length from MBAP header
        if buf.len() >= 6 {
            let length = u16::from_be_bytes([buf[4], buf[5]]) as usize;
            while buf.len() < 7 + length {
                match tokio::time::timeout(
                    Duration::from_millis(self.config.timeout_ms),
                    tokio::io::AsyncReadExt::read_exact(&mut stream, &mut byte),
                )
                .await
                {
                    Ok(Ok(_)) => buf.push(byte[0]),
                    _ => return Err(anyhow::anyhow!("Timeout reading response")),
                }
            }
        }

        if let Some(response) = ModbusTcpResponse::parse(&buf) {
            if response.is_error {
                return Err(anyhow::anyhow!(
                    "Modbus error: function={}, code={}",
                    response.function_code,
                    response.error_code
                ));
            }

            match point.function {
                ModbusFunction::ReadHoldingRegisters | ModbusFunction::ReadInputRegisters => {
                    let values = response.register_values();
                    Ok(serde_json::json!(values))
                }
                ModbusFunction::ReadCoils | ModbusFunction::ReadDiscreteInputs => {
                    let values = response.coil_values(point.quantity);
                    Ok(serde_json::json!(values))
                }
                _ => Ok(serde_json::json!(null)),
            }
        } else {
            Err(anyhow::anyhow!("Failed to parse Modbus response"))
        }
    }

    fn simulate_values(&self, point: &ModbusPoint) -> Result<serde_json::Value> {
        match point.function {
            ModbusFunction::ReadHoldingRegisters | ModbusFunction::ReadInputRegisters => {
                let values: Vec<u16> = (0..point.quantity).map(|i| point.address + i).collect();
                Ok(serde_json::json!(values))
            }
            ModbusFunction::ReadCoils | ModbusFunction::ReadDiscreteInputs => {
                let values: Vec<bool> = (0..point.quantity).map(|i| (i % 2) == 0).collect();
                Ok(serde_json::json!(values))
            }
            _ => Ok(serde_json::json!(null)),
        }
    }

    async fn poll_all_points(&self, event_bus: &EventBus) -> Result<usize> {
        let mut count = 0;
        for point in &self.config.points {
            match self.read_point(point).await {
                Ok(values) => {
                    // Check for changes
                    let key = point.name.clone();
                    let changed = {
                        let last = self.last_values.read().unwrap();
                        match last.get(&key) {
                            Some(prev) => prev != &values,
                            None => true,
                        }
                    };

                    if changed {
                        {
                            let mut last = self.last_values.write().unwrap();
                            last.insert(key, values.clone());
                        }
                        let ink = self.create_ink(point, &values);
                        event_bus.publish(ink);
                        count += 1;
                    }
                }
                Err(e) => {
                    error!(
                        "Modbus Tentacle '{}': failed to read {}: {}",
                        self.name, point.name, e
                    );
                }
            }
        }
        Ok(count)
    }
}

#[async_trait]
impl Tentacle for ModbusTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "modbus"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Modbus Tentacle '{}' starting (host:{}, points: {})",
            self.name,
            self.config.host,
            self.config.points.len()
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);
        let transaction_id = Arc::clone(&self.transaction_id);
        let last_values = Arc::clone(&self.last_values);

        tokio::spawn(async move {
            let tentacle = ModbusTentacle {
                name: tentacle_name.clone(),
                config,
                transaction_id,
                last_values,
            };

            loop {
                match tentacle.poll_all_points(&event_bus_clone).await {
                    Ok(count) => {
                        if count > 0 {
                            info!(
                                "Modbus Tentacle '{}': published {} changed points",
                                tentacle_name, count
                            );
                        }
                    }
                    Err(e) => {
                        error!("Modbus Tentacle '{}': poll failed: {}", tentacle_name, e);
                    }
                }

                tokio::time::sleep(Duration::from_millis(tentacle.config.poll_interval_ms)).await;
            }
        });

        // Keep the main task alive
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> ModbusTentacleConfig {
        ModbusTentacleConfig {
            connection: ModbusConnection::Tcp,
            host: "127.0.0.1".to_string(),
            port: 502,
            baudrate: 9600,
            poll_interval_ms: 1000,
            timeout_ms: 1000,
            reconnect_delay_secs: 5,
            topic: "/modbus/data".to_string(),
            points: vec![ModbusPoint {
                name: "temperature".to_string(),
                function: ModbusFunction::ReadHoldingRegisters,
                address: 0,
                quantity: 1,
                slave_id: 1,
                topic: None,
            }],
            transaction_id_start: 1,
        }
    }

    #[test]
    fn test_modbus_frame_serialization() {
        let frame = ModbusTcpFrame::read_holding_registers(1, 1, 0, 10);
        let bytes = frame.serialize();
        assert_eq!(bytes.len(), 12);
        assert_eq!(bytes[0], 0); // transaction_id high
        assert_eq!(bytes[1], 1); // transaction_id low
        assert_eq!(bytes[2], 0); // protocol_id
        assert_eq!(bytes[3], 0);
        assert_eq!(bytes[4], 0); // length high
        assert_eq!(bytes[5], 5); // length low (unit_id + function + 4 data bytes)
        assert_eq!(bytes[6], 1); // unit_id
        assert_eq!(bytes[7], 0x03); // function code
    }

    #[test]
    fn test_modbus_response_parsing() {
        // Simulated response: 3 register values
        let bytes = vec![
            0x00, 0x01, // transaction_id
            0x00, 0x00, // protocol_id
            0x00, 0x07, // length
            0x01, // unit_id
            0x03, // function code
            0x06, // byte count (3 registers * 2 bytes)
            0x00, 0x1E, // register 0 = 30
            0x00, 0x32, // register 1 = 50
            0x00, 0x64, // register 2 = 100
        ];
        let response = ModbusTcpResponse::parse(&bytes).unwrap();
        assert!(!response.is_error);
        assert_eq!(response.function_code, 0x03);
        let values = response.register_values();
        assert_eq!(values, vec![30, 50, 100]);
    }

    #[test]
    fn test_modbus_error_response() {
        let bytes = vec![
            0x00, 0x01, // transaction_id
            0x00, 0x00, // protocol_id
            0x00, 0x03, // length
            0x01, // unit_id
            0x83, // error function code (0x80 | 0x03)
            0x02, // error code: illegal data address
        ];
        let response = ModbusTcpResponse::parse(&bytes).unwrap();
        assert!(response.is_error);
        assert_eq!(response.error_code, 0x02);
    }

    #[test]
    fn test_coil_parsing() {
        let bytes = vec![
            0x00, 0x01, // transaction_id
            0x00, 0x00, // protocol_id
            0x00, 0x04, // length
            0x01, // unit_id
            0x01, // function code (read coils)
            0x02, // byte count
            0xFF, 0x00, // 8 coils ON, 8 coils OFF
        ];
        let response = ModbusTcpResponse::parse(&bytes).unwrap();
        let coils = response.coil_values(16);
        assert_eq!(coils.len(), 16);
        assert!(coils[0]); // first 8 should be true
        assert!(!coils[8]); // last 8 should be false
    }

    #[test]
    fn test_tentacle_creation() {
        let config = make_config();
        let tentacle = ModbusTentacle::new("test".to_string(), config);
        assert_eq!(tentacle.name(), "test");
        assert_eq!(tentacle.r#type(), "modbus");
    }

    #[test]
    fn test_point_topic_default() {
        let config = make_config();
        let tentacle = ModbusTentacle::new("test".to_string(), config);
        let point = &tentacle.config.points[0];
        let topic = tentacle.get_point_topic(point);
        assert_eq!(topic, "/modbus/data/temperature");
    }

    #[test]
    fn test_point_topic_custom() {
        let mut config = make_config();
        config.points[0].topic = Some("/sensors/temp".to_string());
        let tentacle = ModbusTentacle::new("test".to_string(), config);
        let point = &tentacle.config.points[0];
        let topic = tentacle.get_point_topic(point);
        assert_eq!(topic, "/sensors/temp");
    }

    #[test]
    fn test_simulate_register_values() {
        let config = make_config();
        let tentacle = ModbusTentacle::new("test".to_string(), config);
        let point = &tentacle.config.points[0];
        let values = tentacle.simulate_values(point).unwrap();
        let arr = values.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].as_u64().unwrap(), 0); // address 0
    }

    #[test]
    fn test_simulate_coil_values() {
        let mut config = make_config();
        config.points[0].function = ModbusFunction::ReadCoils;
        config.points[0].quantity = 4;
        let tentacle = ModbusTentacle::new("test".to_string(), config);
        let point = &tentacle.config.points[0];
        let values = tentacle.simulate_values(point).unwrap();
        let arr = values.as_array().unwrap();
        assert_eq!(arr.len(), 4);
        // Even indices true, odd false
        assert!(arr[0].as_bool().unwrap());
        assert!(!arr[1].as_bool().unwrap());
        assert!(arr[2].as_bool().unwrap());
        assert!(!arr[3].as_bool().unwrap());
    }

    #[test]
    fn test_transaction_id_increment() {
        let config = make_config();
        let tentacle = ModbusTentacle::new("test".to_string(), config);
        let id1 = tentacle.next_transaction_id();
        let id2 = tentacle.next_transaction_id();
        assert_eq!(id1 + 1, id2);
    }

    #[test]
    fn test_function_code_mapping() {
        assert_eq!(ModbusFunction::ReadCoils.code(), 0x01);
        assert_eq!(ModbusFunction::ReadDiscreteInputs.code(), 0x02);
        assert_eq!(ModbusFunction::ReadHoldingRegisters.code(), 0x03);
        assert_eq!(ModbusFunction::ReadInputRegisters.code(), 0x04);
        assert_eq!(ModbusFunction::WriteSingleCoil.code(), 0x05);
        assert_eq!(ModbusFunction::WriteSingleRegister.code(), 0x06);
        assert_eq!(ModbusFunction::WriteMultipleCoils.code(), 0x0F);
        assert_eq!(ModbusFunction::WriteMultipleRegisters.code(), 0x10);
    }

    #[test]
    fn test_ink_creation() {
        let config = make_config();
        let tentacle = ModbusTentacle::new("test".to_string(), config);
        let point = &tentacle.config.points[0];
        let values = serde_json::json!([42]);
        let ink = tentacle.create_ink(point, &values);
        assert_eq!(ink.topic, "/modbus/data/temperature");
        assert_eq!(ink.source.r#type, "modbus");
        assert!(ink.body.contains("temperature"));
    }
}
