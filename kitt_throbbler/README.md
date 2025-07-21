# KITT Throbbler

A simple library for creating a Knight Rider-style throbber animation in the terminal.

## Usage

```rust
use kitt_throbbler::Throbber;

#[tokio::main]
async fn main() {
    let throbber = Throbber::new();
    throbber.start().await;
}
```
