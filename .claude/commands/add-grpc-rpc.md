Add a new gRPC RPC to the pipelineR service: `$ARGUMENTS`

Steps:
1. Read `.claude/skills/grpc-proto/SKILL.md` for proto conventions and tonic patterns
2. Define request/response messages in the appropriate proto file under `pipeliner-proto/proto/pipeliner/v1/`
3. Add the RPC to the service definition in `service.proto` (orchestrator-facing) or `plugin.proto` (plugin-facing)
4. Run `cargo build -p pipeliner-proto` to regenerate Rust code from protos
5. Implement the server handler in `pipeliner-core/src/grpc/` (for orchestrator RPCs) or `pipeliner-sdk/src/` (for plugin RPCs)
6. Add the client call in the corresponding client module
7. Write a test that exercises the new RPC end-to-end (spawn server, call via client, assert response)
8. Update the gRPC service definition in `docs/REQUIREMENTS.md` if this is a new orchestrator-facing RPC
