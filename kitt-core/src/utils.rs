//! Utility functions for KITT
//!
//! This module contains helper functions for common operations like
//! mathematical calculations, byte formatting/parsing, and CRC verification.

use anyhow::{anyhow, Result};
use tracing::debug;

/// Calculate the Greatest Common Divisor of two numbers
pub fn gcd(a: usize, b: usize) -> usize {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

/// Calculate the Least Common Multiple of two numbers
pub fn lcm(a: usize, b: usize) -> usize {
    a * b / gcd(a, b)
}

/// Formats a byte count into a human-readable string (KB, MB, GB, etc.)
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Parses a human-readable byte size string (e.g., "1GB", "500MB", "1024KB") into bytes
pub fn parse_bytes(s: &str) -> Result<u64> {
    let s = s.trim().to_uppercase();

    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    // Find where the numeric part ends
    let numeric_end = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());

    let (num_str, unit) = s.split_at(numeric_end);
    let num: f64 = num_str
        .parse()
        .map_err(|_| anyhow!("Invalid number in size: {}", s))?;

    let multiplier = match unit.trim() {
        "" | "B" => 1,
        "K" | "KB" | "KIB" => KB,
        "M" | "MB" | "MIB" => MB,
        "G" | "GB" | "GIB" => GB,
        "T" | "TB" | "TIB" => TB,
        _ => return Err(anyhow!("Unknown unit in size: {}", unit)),
    };

    Ok((num * multiplier as f64) as u64)
}

/// Verify CRC32-C of a Kafka record batch
///
/// Kafka record batch format (v2):
/// - baseOffset: int64 (8 bytes) - offset 0
/// - batchLength: int32 (4 bytes) - offset 8
/// - partitionLeaderEpoch: int32 (4 bytes) - offset 12
/// - magic: int8 (1 byte) - offset 16
/// - crc: int32 (4 bytes) - offset 17
/// - attributes onwards: covered by CRC - offset 21
///
/// Returns Ok(batch_length) if CRC matches, Err with details if not
pub fn verify_record_batch_crc(data: &[u8]) -> Result<usize> {
    // Minimum size for a record batch header
    if data.len() < 21 {
        return Err(anyhow!("Record batch too short: {} bytes", data.len()));
    }

    // Read batch length (offset 8, 4 bytes, big-endian)
    let batch_length = i32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize;

    // Total batch size = 8 (baseOffset) + 4 (batchLength) + batchLength
    let total_batch_size = 12 + batch_length;
    if data.len() < total_batch_size {
        return Err(anyhow!(
            "Incomplete record batch: expected {} bytes, got {}",
            total_batch_size,
            data.len()
        ));
    }

    // Read magic byte (offset 16)
    let magic = data[16];
    if magic != 2 {
        // Only verify CRC for magic version 2 (modern format)
        debug!("Skipping CRC check for magic version {}", magic);
        return Ok(total_batch_size);
    }

    // Read stored CRC (offset 17, 4 bytes, big-endian)
    let stored_crc = u32::from_be_bytes([data[17], data[18], data[19], data[20]]);

    // Compute CRC32-C over data from attributes (offset 21) to end of batch
    let crc_data = &data[21..total_batch_size];
    let computed_crc = crc32c::crc32c(crc_data);

    if stored_crc != computed_crc {
        return Err(anyhow!(
            "CRC mismatch: stored=0x{:08x}, computed=0x{:08x}",
            stored_crc,
            computed_crc
        ));
    }

    Ok(total_batch_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcd() {
        assert_eq!(gcd(48, 18), 6);
        assert_eq!(gcd(18, 48), 6);
        assert_eq!(gcd(12, 8), 4);
        assert_eq!(gcd(7, 13), 1);
        assert_eq!(gcd(0, 5), 5);
        assert_eq!(gcd(5, 0), 5);
        assert_eq!(gcd(100, 100), 100);
    }

    #[test]
    fn test_lcm() {
        assert_eq!(lcm(4, 6), 12);
        assert_eq!(lcm(3, 5), 15);
        assert_eq!(lcm(12, 18), 36);
        assert_eq!(lcm(7, 7), 7);
        assert_eq!(lcm(1, 10), 10);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 bytes");
        assert_eq!(format_bytes(512), "512 bytes");
        assert_eq!(format_bytes(1023), "1023 bytes");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
        assert_eq!(format_bytes(1610612736), "1.50 GB");
    }

    #[test]
    fn test_parse_bytes_plain_numbers() {
        assert_eq!(parse_bytes("1024").unwrap(), 1024);
        assert_eq!(parse_bytes("100").unwrap(), 100);
        assert_eq!(parse_bytes("0").unwrap(), 0);
    }

    #[test]
    fn test_parse_bytes_with_units() {
        assert_eq!(parse_bytes("1KB").unwrap(), 1024);
        assert_eq!(parse_bytes("1K").unwrap(), 1024);
        assert_eq!(parse_bytes("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_bytes("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_bytes("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_bytes("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_bytes("1TB").unwrap(), 1024 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_bytes_case_insensitive() {
        assert_eq!(parse_bytes("1kb").unwrap(), 1024);
        assert_eq!(parse_bytes("1Kb").unwrap(), 1024);
        assert_eq!(parse_bytes("1mb").unwrap(), 1024 * 1024);
        assert_eq!(parse_bytes("1gb").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_bytes_with_whitespace() {
        assert_eq!(parse_bytes("  1024  ").unwrap(), 1024);
        assert_eq!(parse_bytes("1 KB").unwrap(), 1024);
    }

    #[test]
    fn test_parse_bytes_fractional() {
        assert_eq!(parse_bytes("1.5KB").unwrap(), 1536);
        assert_eq!(parse_bytes("0.5MB").unwrap(), 512 * 1024);
    }

    #[test]
    fn test_parse_bytes_invalid() {
        assert!(parse_bytes("abc").is_err());
        assert!(parse_bytes("1XB").is_err());
        assert!(parse_bytes("").is_err());
    }
}
