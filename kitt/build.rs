// Rust guideline compliant October 17th 2025

//! Build script that converts `Kitt-logo.png` into ANSI art at compile time
//! using the [`logo_art`] crate. The generated text is written to `OUT_DIR/logo.txt`
//! and included in the binary via `include_str!`.

use std::path::Path;
use std::{env, fs};

fn main() {
    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");
    let logo_path = Path::new("src/resources/Kitt-logo.png");

    println!("cargo::rerun-if-changed={}", logo_path.display());

    let image_data = fs::read(logo_path).expect("Failed to read Kitt-logo.png");
    let ansi_art = logo_art::image_to_ansi(&image_data, 100);

    fs::write(Path::new(&out_dir).join("logo.txt"), &ansi_art)
        .expect("Failed to write generated logo.txt");
}
