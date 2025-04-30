# Copy Trade Rust

This is a Rust version of my copy trade logic originally written in Node.js.

During benchmarking, I noticed a 2–4x improvement in fetch speed and copy-buy execution compared to the Node.js version. The project is still under development as I'm currently unfamiliar with the Rust language and learning as I go.

This code is a modified and simplified version of Triton One's official examples. I've removed, adjusted, and refactored files to focus specifically on wallet monitoring and swap execution—features that were not provided in the original examples.

It currently uses three listener types: **gRPC**, **Syndica**, and **Pump** to improve coverage and reduce latency when detecting transactions.

Each listener calls `handle.rs`, but only the first one to call it proceeds with execution. The others are ignored to prevent duplicate swap attempts.
