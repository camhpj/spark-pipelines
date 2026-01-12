# Architecture

Verified on 2026-01-12.

## Core Flow
- `compile_pipeline()` orchestrates parsing, validation, artifact generation, and reporting.
- The compiler loads a single YAML document into `PipelineDocument` (mapping + pipeline + features + profiling).
- A default `SemanticContract` supplies required canonical keys per entity.
- The compiler wipes `out_dir`, recreates the fixed directory layout, then writes models, rendered SQL, compile report, and optional profiling notebook.

## Determinism and Layout
- `out_dir` is fully removed and recreated on each compile.
- Artifact directories are fixed: `models/semantic`, `models/features`, `models/marts`, `tests`, `notebooks`, `rendered`, `manifest`.

## Metadata
- Final SQL is prefixed with comment metadata: pipeline name, version, compile timestamp, and feature list.
