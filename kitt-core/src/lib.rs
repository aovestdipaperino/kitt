#![deny(warnings)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::indexing_slicing)]
// TODO(nasa-rule-12): temporary allows — remove as violations are fixed in subsequent tasks
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::indexing_slicing)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::float_cmp)]
#![allow(clippy::if_not_else)]
#![allow(clippy::if_same_then_else)]
#![allow(clippy::ignored_unit_patterns)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::map_unwrap_or)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::semicolon_if_nothing_returned)]
#![allow(clippy::single_char_pattern)]
#![allow(clippy::single_match_else)]
#![allow(clippy::struct_field_names)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::unused_async)]
//! Kitt Core - Kafka throughput testing library
//!
//! This library provides both high-level and low-level APIs for measuring
//! Kafka producer/consumer throughput.

pub mod client;
pub mod config;
pub mod consts;
pub mod consumer;
pub mod error;
pub mod events;
pub mod producer;
pub mod runner;
pub mod utils;

// Re-export public API
pub use client::{KafkaClient, TopicMetadata};
pub use error::{KittError, Result as KittResult};
pub use config::{KeyStrategy, MessageSize, ProduceOnlyMode, TestConfig, TestConfigBuilder};
pub use consumer::Consumer;
pub use events::{TestEvent, TestPhase, TestResults};
pub use producer::Producer;
pub use runner::{run_test, TestHandle};
