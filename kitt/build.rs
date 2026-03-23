//! Build script that converts `Kitt-logo.png` into ANSI art at compile time
//! using the [`logo_art`] crate. The generated text is written to `OUT_DIR/logo.txt`
//! and included in the binary via `include_str!`.

use std::path::PathBuf;
use std::{env, fs};

/// Character-column width of the generated ANSI art logo.
const LOGO_WIDTH: u32 = 100;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let logo_path = manifest_dir.join("src/resources/Kitt-logo.png");

    println!("cargo::rerun-if-changed={}", logo_path.display());

    let image_data =
        fs::read(&logo_path).unwrap_or_else(|e| panic!("Failed to read {}: {e}", logo_path.display()));
    let ansi_art = logo_art::image_to_ansi(&image_data, LOGO_WIDTH);

    let out_path = out_dir.join("logo.txt");
    fs::write(&out_path, &ansi_art)
        .unwrap_or_else(|e| panic!("Failed to write {}: {e}", out_path.display()));
}
