//! OLED display module for SSD1306 128x64 I2C display
//!
//! This module handles displaying battery voltage, device count, and LoRaWAN status
//! on a 0.96 inch SSD1306 OLED display connected via I2C.

use defmt::{debug, error, info};
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::mutex::Mutex;
use embassy_sync::signal::Signal;
use embassy_time::{Duration, Timer};
use embedded_graphics::{
    mono_font::{ascii::FONT_6X12, MonoTextStyle},
    pixelcolor::BinaryColor,
    prelude::*,
    text::{Baseline, Text},
};
use esp_hal::Async;
use ssd1306::{prelude::*, Ssd1306Async};

use crate::battery::BatteryMonitor;
use crate::VictronDeviceStorage;

/// LoRaWAN connection state for display
#[derive(Clone, Copy, Debug, defmt::Format)]
pub enum LoRaWanState {
    /// Attempting to join the network
    Joining,
    /// Successfully connected to the network
    Connected,
    /// Error or disconnected
    Error,
}

/// Static signal for LoRaWAN state updates
pub static LORAWAN_STATE: Signal<CriticalSectionRawMutex, LoRaWanState> = Signal::new();

/// Display task - updates OLED display every 5 seconds
///
/// Displays:
/// - Line 1: Battery voltage and device count (e.g., "3.87V | Devices: 5")
/// - Line 2: LoRaWAN connection state (e.g., "LoRa: OK")
#[embassy_executor::task]
pub async fn display_task(
    i2c: esp_hal::i2c::master::I2c<'static, Async>,
    device_storage: &'static Mutex<CriticalSectionRawMutex, VictronDeviceStorage>,
    battery_monitor: &'static Mutex<CriticalSectionRawMutex, BatteryMonitor<'static, esp_hal::peripherals::GPIO1<'static>>>,
) {
    info!("Display task starting...");

    // Wait a bit for system to initialize
    Timer::after(Duration::from_secs(1)).await;

    // Initialize display interface
    info!("Creating I2C display interface on GPIO17(SDA)/GPIO18(SCL)");
    info!("Hardware control: GPIO36(power,LOW), GPIO21(reset,LOW)");
    debug!("I2C address will be 0x3C (default for SSD1306)");
    let interface = ssd1306::I2CDisplayInterface::new(i2c);

    // Create display driver (128x64, no rotation)
    info!("Creating SSD1306 driver (128x64, no rotation)");
    let mut display = Ssd1306Async::new(interface, DisplaySize128x64, DisplayRotation::Rotate0)
        .into_buffered_graphics_mode();

    // Initialize the display
    info!("Initializing display hardware (sending init sequence)");
    if let Err(_) = display.init().await {
        error!("Failed to initialize display - I2C communication error");
        error!("Troubleshooting steps:");
        error!("  1. Verify GPIO36 is LOW (power enabled)");
        error!("  2. Verify GPIO21 is LOW (reset released)");
        error!("  3. Check I2C connections: GPIO17(SDA), GPIO18(SCL)");
        error!("  4. Verify display power supply (3.3V)");
        error!("  5. Check I2C address (0x3C or 0x3D)");
        return;
    }

    info!("Display initialized successfully");

    // Clear the display
    debug!("Clearing display framebuffer");
    display.clear(BinaryColor::Off).ok();
    debug!("Sending initial clear to display via I2C");
    if let Err(_) = display.flush().await {
        error!("Failed to clear display - I2C write error");
        error!("Display hardware responded to init but flush failed");
    } else {
        info!("Display cleared and ready");
    }

    // Text style with large font
    let text_style = MonoTextStyle::new(&FONT_6X12, BinaryColor::On);

    // Main display update loop
    loop {
        debug!("Display update cycle starting");

        // Read battery voltage
        let battery_mv = {
            match battery_monitor.try_lock() {
                Ok(mut monitor) => {
                    let voltage = monitor.read_voltage_mv().await;
                    debug!("Battery voltage read: {} mV", voltage);
                    voltage
                }
                Err(_) => {
                    debug!("Failed to lock battery monitor");
                    0
                }
            }
        };

        // Count valid devices
        let device_count = {
            match device_storage.try_lock() {
                Ok(storage) => {
                    let count = storage.devices.iter().filter(|d| d.valid).count();
                    debug!("Device count: {}", count);
                    count
                }
                Err(_) => {
                    debug!("Failed to lock device storage");
                    0
                }
            }
        };

        // Get LoRaWAN state
        let lora_state = LORAWAN_STATE.signaled();
        let lora_state_value = if lora_state {
            // Peek at the current state without waiting
            LORAWAN_STATE.try_take().unwrap_or(LoRaWanState::Joining)
        } else {
            LoRaWanState::Joining
        };

        // Put it back if we took it
        if lora_state {
            LORAWAN_STATE.signal(lora_state_value);
        }
        debug!("LoRaWAN state: {:?}", lora_state_value);

        // Format strings for display
        let mut line1_buffer = heapless::String::<32>::new();
        let mut line2_buffer = heapless::String::<32>::new();

        // Line 1: Battery voltage and device count
        use core::fmt::Write;
        let _ = write!(
            &mut line1_buffer,
            "{}.{:02}V | Dev: {}",
            battery_mv / 1000,
            (battery_mv % 1000) / 10,
            device_count
        );

        // Line 2: LoRaWAN status
        let lora_text = match lora_state_value {
            LoRaWanState::Joining => "LoRa: Joining",
            LoRaWanState::Connected => "LoRa: OK",
            LoRaWanState::Error => "LoRa: Error",
        };
        let _ = write!(&mut line2_buffer, "{}", lora_text);

        debug!("Display text - Line 1: '{}', Line 2: '{}'", line1_buffer.as_str(), line2_buffer.as_str());

        // Clear display
        display.clear(BinaryColor::Off).ok();

        // Draw text with baseline at top (so Y coordinate is the top of the text)
        debug!("Drawing text to display buffer");
        Text::with_baseline(&line1_buffer, Point::new(0, 16), text_style, Baseline::Top)
            .draw(&mut display)
            .ok();

        Text::with_baseline(&line2_buffer, Point::new(0, 48), text_style, Baseline::Top)
            .draw(&mut display)
            .ok();

        // Flush to display
        debug!("Flushing display buffer to hardware via I2C");
        if let Err(_) = display.flush().await {
            error!("Failed to update display - I2C write error");
            error!("Check display connections, power, and I2C bus");
        } else {
            debug!("Display updated successfully");
        }

        // Update every 5 seconds
        Timer::after(Duration::from_secs(5)).await;
    }
}
