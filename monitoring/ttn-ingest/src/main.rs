use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use influxdb2::models::DataPoint;
use influxdb2::Client;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

// TTN API response structures — only the fields we actually consume.
#[derive(Debug, Deserialize)]
struct TtnMessage {
    result: UplinkResult,
}

#[derive(Debug, Deserialize)]
struct UplinkResult {
    end_device_ids: EndDeviceIds,
    received_at: String,
    uplink_message: UplinkMessage,
}

#[derive(Debug, Deserialize)]
struct EndDeviceIds {
    device_id: String,
}

#[derive(Debug, Deserialize)]
struct UplinkMessage {
    #[serde(default)]
    f_cnt: Option<u32>,
    #[serde(default)]
    decoded_payload: Option<serde_json::Value>,
    #[serde(default)]
    rx_metadata: Vec<RxMetadata>,
    #[serde(default)]
    settings: Option<Settings>,
}

#[derive(Debug, Deserialize)]
struct RxMetadata {
    gateway_ids: GatewayIds,
    #[serde(default)]
    rssi: Option<i32>,
    #[serde(default)]
    snr: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct GatewayIds {
    gateway_id: String,
}

#[derive(Debug, Deserialize)]
struct Settings {
    #[serde(default)]
    data_rate: Option<DataRate>,
}

#[derive(Debug, Deserialize)]
struct DataRate {
    #[serde(default)]
    lora: Option<LoRaSettings>,
}

#[derive(Debug, Deserialize)]
struct LoRaSettings {
    #[serde(default)]
    bandwidth: Option<u32>,
    #[serde(default)]
    spreading_factor: Option<u8>,
}

// Device dispatch table. Adding a new Victron device is a one-entry change.
#[derive(Clone, Copy)]
enum FieldType {
    F64,
    I64,
}

struct FieldMap {
    json_key: &'static str,
    influx_field: &'static str,
    ty: FieldType,
}

struct DeviceSpec {
    device_type: &'static str,
    measurement: &'static str,
    fields: &'static [FieldMap],
}

const DEVICE_SPECS: &[DeviceSpec] = &[
    DeviceSpec {
        device_type: "BatteryMonitor",
        measurement: "battery_monitor",
        fields: &[
            FieldMap {
                json_key: "voltage",
                influx_field: "voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "current",
                influx_field: "current",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "soc",
                influx_field: "soc",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "consumed_ah",
                influx_field: "consumed_ah",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "rssi",
                influx_field: "device_rssi",
                ty: FieldType::I64,
            },
        ],
    },
    DeviceSpec {
        device_type: "SolarCharger",
        measurement: "solar_charger",
        fields: &[
            FieldMap {
                json_key: "battery_voltage",
                influx_field: "battery_voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "battery_current",
                influx_field: "battery_current",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "pv_power",
                influx_field: "pv_power",
                ty: FieldType::I64,
            },
            FieldMap {
                json_key: "yield_today",
                influx_field: "yield_today",
                ty: FieldType::I64,
            },
            FieldMap {
                json_key: "rssi",
                influx_field: "device_rssi",
                ty: FieldType::I64,
            },
        ],
    },
    DeviceSpec {
        device_type: "DcDcConverter",
        measurement: "dc_dc_converter",
        fields: &[
            FieldMap {
                json_key: "input_voltage",
                influx_field: "input_voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "output_voltage",
                influx_field: "output_voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "off_reason",
                influx_field: "off_reason",
                ty: FieldType::I64,
            },
            FieldMap {
                json_key: "rssi",
                influx_field: "device_rssi",
                ty: FieldType::I64,
            },
        ],
    },
    DeviceSpec {
        device_type: "AcCharger",
        measurement: "ac_charger",
        fields: &[
            FieldMap {
                json_key: "output_voltage1",
                influx_field: "output_voltage1",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "output_current1",
                influx_field: "output_current1",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "output_voltage2",
                influx_field: "output_voltage2",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "output_current2",
                influx_field: "output_current2",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "output_voltage3",
                influx_field: "output_voltage3",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "output_current3",
                influx_field: "output_current3",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "temperature",
                influx_field: "temperature",
                ty: FieldType::I64,
            },
            FieldMap {
                json_key: "ac_current",
                influx_field: "ac_current",
                ty: FieldType::F64,
            },
        ],
    },
    DeviceSpec {
        device_type: "BatterySense",
        measurement: "battery_sense",
        fields: &[
            FieldMap {
                json_key: "voltage",
                influx_field: "voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "temperature",
                influx_field: "temperature",
                ty: FieldType::F64,
            },
        ],
    },
    DeviceSpec {
        device_type: "DcEnergyMeter",
        measurement: "dc_energy_meter",
        fields: &[
            FieldMap {
                json_key: "voltage",
                influx_field: "voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "current",
                influx_field: "current",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "power",
                influx_field: "power",
                ty: FieldType::I64,
            },
        ],
    },
    DeviceSpec {
        device_type: "Inverter",
        measurement: "inverter",
        fields: &[
            FieldMap {
                json_key: "battery_voltage",
                influx_field: "battery_voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "ac_apparent_power",
                influx_field: "ac_apparent_power",
                ty: FieldType::I64,
            },
            FieldMap {
                json_key: "ac_voltage",
                influx_field: "ac_voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "ac_current",
                influx_field: "ac_current",
                ty: FieldType::F64,
            },
        ],
    },
    DeviceSpec {
        device_type: "LynxSmartBMS",
        measurement: "lynx_smart_bms",
        fields: &[
            FieldMap {
                json_key: "voltage",
                influx_field: "voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "current",
                influx_field: "current",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "soc",
                influx_field: "soc",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "consumed_ah",
                influx_field: "consumed_ah",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "temperature",
                influx_field: "temperature",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "battery_temperature",
                influx_field: "battery_temperature",
                ty: FieldType::F64,
            },
        ],
    },
    DeviceSpec {
        device_type: "OrionXS",
        measurement: "orion_xs",
        fields: &[
            FieldMap {
                json_key: "input_voltage",
                influx_field: "input_voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "input_current",
                influx_field: "input_current",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "output_voltage",
                influx_field: "output_voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "output_current",
                influx_field: "output_current",
                ty: FieldType::F64,
            },
        ],
    },
    DeviceSpec {
        device_type: "SmartBatteryProtect",
        measurement: "smart_battery_protect",
        fields: &[
            FieldMap {
                json_key: "input_voltage",
                influx_field: "input_voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "output_voltage",
                influx_field: "output_voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "error_code",
                influx_field: "error_code",
                ty: FieldType::I64,
            },
        ],
    },
    DeviceSpec {
        device_type: "SmartLithium",
        measurement: "smart_lithium",
        fields: &[
            FieldMap {
                json_key: "battery_voltage",
                influx_field: "battery_voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "cell_count",
                influx_field: "cell_count",
                ty: FieldType::I64,
            },
            FieldMap {
                json_key: "temperature",
                influx_field: "temperature",
                ty: FieldType::F64,
            },
        ],
    },
    DeviceSpec {
        device_type: "VEBus",
        measurement: "vebus",
        fields: &[
            FieldMap {
                json_key: "battery_voltage",
                influx_field: "battery_voltage",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "battery_current",
                influx_field: "battery_current",
                ty: FieldType::F64,
            },
            FieldMap {
                json_key: "soc",
                influx_field: "soc",
                ty: FieldType::I64,
            },
            FieldMap {
                json_key: "ac_in_power",
                influx_field: "ac_in_power",
                ty: FieldType::I64,
            },
            FieldMap {
                json_key: "ac_out_power",
                influx_field: "ac_out_power",
                ty: FieldType::I64,
            },
            FieldMap {
                json_key: "battery_temperature",
                influx_field: "battery_temperature",
                ty: FieldType::I64,
            },
        ],
    },
];

fn build_device_point(
    device_id: &str,
    decoded: &serde_json::Value,
    timestamp: DateTime<Utc>,
) -> Result<Option<DataPoint>> {
    let device_type = decoded
        .get("device_type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let ts_nanos = timestamp.timestamp_nanos_opt().unwrap_or(0);

    if let Some(spec) = DEVICE_SPECS.iter().find(|s| s.device_type == device_type) {
        let mut builder = DataPoint::builder(spec.measurement)
            .tag("device_id", device_id)
            .timestamp(ts_nanos);
        let mut has_any_field = false;
        for fm in spec.fields {
            let Some(v) = decoded.get(fm.json_key) else {
                continue;
            };
            match fm.ty {
                FieldType::F64 => {
                    if let Some(n) = v.as_f64() {
                        builder = builder.field(fm.influx_field, n);
                        has_any_field = true;
                    }
                }
                FieldType::I64 => {
                    if let Some(n) = v.as_i64() {
                        builder = builder.field(fm.influx_field, n);
                        has_any_field = true;
                    }
                }
            }
        }
        if !has_any_field {
            return Ok(None);
        }
        Ok(Some(builder.build()?))
    } else {
        warn!("Unknown device type: {}", device_type);
        Ok(Some(
            DataPoint::builder("unknown_device")
                .tag("device_id", device_id)
                .tag("device_type", device_type)
                .field("raw_json", decoded.to_string())
                .timestamp(ts_nanos)
                .build()?,
        ))
    }
}

// Persistent cursor of the most recent `received_at` we have processed.
// Used to ask TTN for `after=<ts>` on reconnect instead of re-fetching
// the trailing 24 hours every time.
#[derive(Clone)]
struct Cursor {
    last_received_at: Arc<Mutex<Option<DateTime<Utc>>>>,
    path: Arc<PathBuf>,
}

impl Cursor {
    fn load(path: PathBuf) -> Self {
        let last = fs::read_to_string(&path)
            .ok()
            .and_then(|s| DateTime::parse_from_rfc3339(s.trim()).ok())
            .map(|dt| dt.with_timezone(&Utc));
        match &last {
            Some(dt) => info!("Loaded cursor from {:?}: {}", path, dt),
            None => info!(
                "No cursor at {:?}; first connect will request last=24h",
                path
            ),
        }
        Self {
            last_received_at: Arc::new(Mutex::new(last)),
            path: Arc::new(path),
        }
    }

    fn get(&self) -> Option<DateTime<Utc>> {
        self.last_received_at.lock().ok().and_then(|g| *g)
    }

    fn update(&self, ts: DateTime<Utc>) {
        let should_persist = {
            let Ok(mut guard) = self.last_received_at.lock() else {
                return;
            };
            match *guard {
                Some(current) if ts <= current => false,
                _ => {
                    *guard = Some(ts);
                    true
                }
            }
        };
        if !should_persist {
            return;
        }
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = fs::create_dir_all(parent);
            }
        }
        let tmp = self.path.with_extension("tmp");
        if let Err(e) = fs::write(&tmp, ts.to_rfc3339()) {
            warn!("Failed to write cursor tmp file: {:?}", e);
            return;
        }
        if let Err(e) = fs::rename(&tmp, &*self.path) {
            warn!("Failed to rename cursor tmp file: {:?}", e);
        }
    }
}

// Health tracking structure
#[derive(Clone)]
struct HealthStatus {
    last_message_time: Arc<RwLock<Instant>>,
    messages_processed: Arc<AtomicU64>,
    reconnect_count: Arc<AtomicU64>,
    start_time: Instant,
}

impl HealthStatus {
    fn new() -> Self {
        Self {
            last_message_time: Arc::new(RwLock::new(Instant::now())),
            messages_processed: Arc::new(AtomicU64::new(0)),
            reconnect_count: Arc::new(AtomicU64::new(0)),
            start_time: Instant::now(),
        }
    }

    fn update_message_received(&self) {
        if let Ok(mut last_time) = self.last_message_time.write() {
            *last_time = Instant::now();
        }
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
    }

    fn update_reconnect(&self) {
        self.reconnect_count.fetch_add(1, Ordering::Relaxed);
    }

    fn is_healthy(&self) -> bool {
        if let Ok(last_time) = self.last_message_time.read() {
            last_time.elapsed() < Duration::from_secs(300)
        } else {
            false
        }
    }

    fn get_status(&self) -> HealthResponse {
        let last_message_ago = if let Ok(last_time) = self.last_message_time.read() {
            last_time.elapsed().as_secs()
        } else {
            0
        };
        HealthResponse {
            status: if self.is_healthy() {
                "healthy"
            } else {
                "unhealthy"
            }
            .to_string(),
            last_message_ago_secs: last_message_ago,
            messages_processed: self.messages_processed.load(Ordering::Relaxed),
            reconnect_count: self.reconnect_count.load(Ordering::Relaxed),
            uptime_secs: self.start_time.elapsed().as_secs(),
        }
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    last_message_ago_secs: u64,
    messages_processed: u64,
    reconnect_count: u64,
    uptime_secs: u64,
}

struct TtnIngestor {
    influx_client: Client,
    ttn_app_id: String,
    ttn_api_key: String,
    ttn_region: String,
    bucket: String,
    health_status: HealthStatus,
    cursor: Cursor,
    stream_connected_at: Arc<Mutex<Option<Instant>>>,
}

impl TtnIngestor {
    fn new() -> Result<Self> {
        let influx_url =
            env::var("INFLUXDB_URL").unwrap_or_else(|_| "http://localhost:8086".to_string());
        let influx_token = env::var("INFLUXDB_TOKEN").context("INFLUXDB_TOKEN not set")?;
        let influx_org =
            env::var("INFLUXDB_ORG").unwrap_or_else(|_| "victron-monitoring".to_string());
        let bucket = env::var("INFLUXDB_BUCKET").unwrap_or_else(|_| "victron-data".to_string());

        let ttn_app_id = env::var("TTN_APP_ID").unwrap_or_else(|_| "vanman".to_string());
        let ttn_api_key = env::var("TTN_API_KEY").context("TTN_API_KEY not set")?;
        let ttn_region = env::var("TTN_REGION").unwrap_or_else(|_| "eu1".to_string());

        let cursor_path = env::var("TTN_CURSOR_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/var/lib/ttn-ingest/cursor"));
        let cursor = Cursor::load(cursor_path);

        let influx_client = Client::new(&influx_url, &influx_org, &influx_token);

        info!("Initialized TTN Ingestor");
        info!("  InfluxDB: {}", influx_url);
        info!("  TTN App: {} ({})", ttn_app_id, ttn_region);
        info!("  Bucket: {}", bucket);

        Ok(Self {
            influx_client,
            ttn_app_id,
            ttn_api_key,
            ttn_region,
            bucket,
            health_status: HealthStatus::new(),
            cursor,
            stream_connected_at: Arc::new(Mutex::new(None)),
        })
    }

    async fn process_uplink(&self, message: TtnMessage) -> Result<()> {
        let device_id = message.result.end_device_ids.device_id.clone();
        let timestamp = DateTime::parse_from_rfc3339(&message.result.received_at)
            .context("Failed to parse timestamp")?
            .with_timezone(&Utc);

        debug!("Processing uplink from {}", device_id);

        let uplink = &message.result.uplink_message;
        let ts_nanos = timestamp.timestamp_nanos_opt().unwrap_or(0);
        let mut points: Vec<DataPoint> = Vec::new();

        for rx in &uplink.rx_metadata {
            if let (Some(rssi), Some(snr)) = (rx.rssi, rx.snr) {
                points.push(
                    DataPoint::builder("signal_quality")
                        .tag("device_id", &device_id)
                        .tag("gateway_id", &rx.gateway_ids.gateway_id)
                        .field("rssi", rssi as i64)
                        .field("snr", snr)
                        .timestamp(ts_nanos)
                        .build()?,
                );
            }
        }

        let mut info_builder = DataPoint::builder("network_info")
            .tag("device_id", &device_id)
            .timestamp(ts_nanos);
        let mut info_has_fields = false;
        if let Some(f_cnt) = uplink.f_cnt {
            info_builder = info_builder.field("f_cnt", f_cnt as i64);
            info_has_fields = true;
        }
        if let Some(lora) = uplink
            .settings
            .as_ref()
            .and_then(|s| s.data_rate.as_ref())
            .and_then(|dr| dr.lora.as_ref())
        {
            if let Some(sf) = lora.spreading_factor {
                info_builder = info_builder.field("spreading_factor", sf as i64);
                info_has_fields = true;
            }
            if let Some(bw) = lora.bandwidth {
                info_builder = info_builder.field("bandwidth", bw as i64);
                info_has_fields = true;
            }
        }
        if info_has_fields {
            points.push(info_builder.build()?);
        }

        if let Some(decoded) = &uplink.decoded_payload {
            if let Some(p) = build_device_point(&device_id, decoded, timestamp)? {
                points.push(p);
            }
        }

        if !points.is_empty() {
            self.influx_client
                .write(&self.bucket, futures::stream::iter(points))
                .await
                .context("Failed to write points to InfluxDB")?;
        }

        info!("✓ Processed uplink from {}", device_id);
        self.health_status.update_message_received();
        self.cursor.update(timestamp);
        Ok(())
    }

    async fn stream_uplinks(&self) -> Result<()> {
        let url = format!(
            "https://{}.cloud.thethings.network/api/v3/as/applications/{}/packages/storage/uplink_message",
            self.ttn_region, self.ttn_app_id
        );

        let mut query: Vec<(&str, String)> = Vec::new();
        match self.cursor.get() {
            Some(ts) => {
                let after = ts.to_rfc3339();
                info!("Connecting to TTN: {} (after={})", url, after);
                query.push(("after", after));
            }
            None => {
                info!("Connecting to TTN: {} (last=24h)", url);
                query.push(("last", "24h".to_string()));
            }
        }
        // Ask TTN to only send the uplink_message fields we consume. Paths
        // are rooted at `up` in the storage integration's gRPC schema
        // (which is `result` in the JSON envelope). TTN expects a single
        // comma-separated value, and end_device_ids/received_at are always
        // included and rejected from the mask.
        query.push((
            "field_mask",
            [
                "up.uplink_message.f_cnt",
                "up.uplink_message.decoded_payload",
                "up.uplink_message.rx_metadata",
                "up.uplink_message.settings",
            ]
            .join(","),
        ));

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .pool_max_idle_per_host(0)
            .pool_idle_timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(10))
            .build()?;

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.ttn_api_key))
            .header("Accept", "text/event-stream")
            .query(&query)
            .send()
            .await
            .context("Failed to connect to TTN")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read body>".to_string());
            anyhow::bail!("TTN API returned status {}: {}", status, body);
        }

        info!("Connected to TTN stream");
        if let Ok(mut g) = self.stream_connected_at.lock() {
            *g = Some(Instant::now());
        }

        let mut stream = response.bytes_stream();
        let mut buffer = String::new();

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    let text = String::from_utf8_lossy(&bytes);
                    buffer.push_str(&text);

                    while let Some(newline_pos) = buffer.find('\n') {
                        let line = buffer[..newline_pos].trim().to_string();
                        buffer = buffer[newline_pos + 1..].to_string();

                        if line.is_empty() {
                            continue;
                        }

                        let json_str = if line.starts_with("data:") {
                            line.strip_prefix("data:").unwrap_or(&line).trim()
                        } else {
                            &line
                        };

                        if json_str.is_empty() {
                            continue;
                        }

                        match serde_json::from_str::<TtnMessage>(json_str) {
                            Ok(message) => {
                                let device_id = message.result.end_device_ids.device_id.clone();
                                if let Err(e) = self.process_uplink(message).await {
                                    error!("Failed to process uplink from {}: {:?}", device_id, e);
                                }
                            }
                            Err(e) => {
                                if !json_str.starts_with(':') {
                                    debug!(
                                        "Failed to parse JSON (might be keep-alive): {:?}, raw: {}",
                                        e,
                                        json_str.chars().take(100).collect::<String>()
                                    );
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Stream error: {:?}", e);
                    anyhow::bail!("Stream interrupted: {:?}", e);
                }
            }
        }

        warn!("Stream ended");
        Ok(())
    }

    async fn run(&self) -> Result<()> {
        let mut attempt: u32 = 0;
        loop {
            match self.stream_uplinks().await {
                Ok(_) => warn!("Stream ended normally, reconnecting..."),
                Err(e) => {
                    self.health_status.update_reconnect();
                    error!("Stream error (attempt {}): {:?}", attempt + 1, e);
                }
            }

            // If the stream stayed connected long enough to count as a
            // successful session, reset the backoff counter.
            let stayed_connected = self
                .stream_connected_at
                .lock()
                .ok()
                .and_then(|g| *g)
                .map(|t| t.elapsed() >= Duration::from_secs(60))
                .unwrap_or(false);
            if let Ok(mut g) = self.stream_connected_at.lock() {
                *g = None;
            }
            if stayed_connected {
                attempt = 0;
            } else {
                attempt = attempt.saturating_add(1);
            }

            // Exponential backoff: 2s, 4s, 8s, ... capped at 300s, plus up to 1s jitter.
            let exp = attempt.saturating_sub(1).min(8); // 2^8 = 256
            let delay_secs = (2u64.saturating_mul(1u64 << exp)).min(300);
            let jitter_millis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| (d.subsec_nanos() % 1000) as u64)
                .unwrap_or(0);
            let total = Duration::from_secs(delay_secs) + Duration::from_millis(jitter_millis);
            warn!("Reconnecting in {:?} (next attempt {})", total, attempt + 1);
            tokio::time::sleep(total).await;
        }
    }
}

// Health check HTTP server
async fn run_health_server(health_status: HealthStatus) {
    let listener = match TcpListener::bind("0.0.0.0:8080").await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind health server: {:?}", e);
            return;
        }
    };

    info!("Health server listening on http://0.0.0.0:8080");

    loop {
        match listener.accept().await {
            Ok((mut socket, _)) => {
                let health = health_status.clone();
                tokio::spawn(async move {
                    let mut buffer = [0; 512];
                    if let Ok(n) = socket.read(&mut buffer).await {
                        if n > 0 {
                            let request = String::from_utf8_lossy(&buffer[..n]);
                            if request.contains("GET /health") {
                                let status = health.get_status();
                                let json = serde_json::to_string(&status)
                                    .unwrap_or_else(|_| "{}".to_string());
                                let http_status = if health.is_healthy() {
                                    "200 OK"
                                } else {
                                    "503 Service Unavailable"
                                };
                                let response = format!(
                                    "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                    http_status,
                                    json.len(),
                                    json
                                );
                                let _ = socket.write_all(response.as_bytes()).await;
                            } else {
                                let response =
                                    "HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found";
                                let _ = socket.write_all(response.as_bytes()).await;
                            }
                        }
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {:?}", e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("Starting TTN Data Ingestor");

    let ingestor = TtnIngestor::new()?;

    let health_status = ingestor.health_status.clone();
    tokio::spawn(async move {
        run_health_server(health_status).await;
    });

    ingestor.run().await
}
