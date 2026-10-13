# Provenance and Showcase Scope

[中文](README.md) | [English overview](../README_en.md)

The code is selected from the custom Codex Router project: 18 Go source and test files across eight packages, preserving the implementation and its existing tests.

## Adjustments for Independent Reading and Testing

- `go.mod` retains the original module name and Go 1.26 directive to keep import paths intact. The selected packages need no external Go modules, so unused dependencies are omitted.
- `scripts/test.py` provides an offline test entry point using temporary databases and caches; test identities and messages are synthetic.
- Chinese and English documentation explain the selected implementation's purpose, mechanisms, and integration boundaries.

Online platform adapters, the real Codex client, and deployment and session-management tools are not included. The repository contains no runtime data, private configuration, or vendored third-party copies. See the [architecture description](../docs_en/ARCHITECTURE.md) for its place in the complete design.

The selected Router code has no accompanying reuse license. This repository supports inspection of the design and implementation; it grants no additional code-reuse permission.
