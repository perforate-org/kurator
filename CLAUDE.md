# OleaphDB: rules for contributors and agents

日本語でやりとりする。コードとドキュメントのコメントは英語。

## Design principles

- DRY: one definition per rule or format (e.g. `oleaph_wal::LsnFiles` names every
  LSN-numbered file).
- Behaviour lives in types: enums, newtypes and typestates, not `bool` or strings
  (`Health`, `Place`, `TailDamage`, `Encoded`). `max-struct-bools = 0` and
  `max-fn-params-bools = 0` enforce part of this.
- Encapsulation: private fields, `#[non_exhaustive]` public types, narrow
  interfaces. No `pub(crate)` fields.
- RAII for resources: `DirLock`, `GroupCommit` (joins its thread on drop),
  temporary snapshot files.
- No `mod.rs`; use `foo.rs` + `foo/`.
- Library code never panics: no `unwrap`, `expect`, indexing or unchecked
  arithmetic.
- Decide performance questions with benchmarks (`cargo bench`).

## Gate (all must pass; CI runs them)

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps          # RUSTDOCFLAGS=-D warnings
deslop-please                            # needs rust-analyzer + rust-src
qlty check --all && qlty smells --all
cargo kani --workspace                   # formal proofs (#[cfg(kani)] modules)
cargo mutants --workspace                # mutation testing
specs/check.sh                           # Quint specs (quint, Java 17+)
```

- Errors flagged by `deslop-please` must be fixed from first principles, not by
  mechanical moves that hide them. `module-cycle` and `tangle`: which module
  belongs at which layer? `field-leak` and `shallow-module`: why do these fields
  or functions exist, and can they hide behind a narrow interface?
- Lints are not silenced with `allow`. Where an exception is justified, use
  `#[expect(lint, reason = "...")]`.
- Property tests (proptest) for codecs and for the database against a model;
  Kani proofs for parsing totality and round trips. Use Verus when a proof
  needs more than bounded model checking.
- Protocols (consensus, cross-shard commit) get a Quint spec in `specs/`
  that names the code it models; a change to the protocol changes the spec
  in the same pull request.
