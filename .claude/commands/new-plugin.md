Scaffold a new pipelineR plugin: `$ARGUMENTS`

Steps:
1. Read `.claude/skills/plugin-sdk/SKILL.md` for the plugin contract and patterns
2. Create the crate directory: `plugins/pipeliner-plugin-$ARGUMENTS/`
3. Create `Cargo.toml` with dependencies on `pipeliner-sdk`, `pipeliner-proto`, `tonic`, `tokio`, `serde`, `serde_json`
4. Create `src/main.rs` with the `run_plugin` scaffold from the SDK skill
5. Create `src/config.rs` with a `#[derive(Deserialize)]` config struct
6. Create `src/source.rs` (if source) implementing `Source` trait with TODO stubs for each method
7. Create `src/sink.rs` (if sink) implementing `Sink` trait with TODO stubs for each method
8. Add the crate to the workspace `Cargo.toml`
9. Verify `cargo check -p pipeliner-plugin-$ARGUMENTS` compiles
10. Create a basic integration test in `tests/integration.rs` that spawns the plugin and calls `Describe`
