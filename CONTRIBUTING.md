# Contributing to sqlmq

Thanks for your interest. sqlmq is in active design — see `docs/superpowers/specs/` for the current design spec before opening anything substantial.

## Before opening a PR

1. **Open an issue first** for anything beyond a typo or one-line fix. We'd rather discuss the approach before you spend time on it.
2. **Check the design spec.** Many things that look like bugs or missing features are deliberate (no server-side `read_with_poll`, no down migrations, no top-level `content_type` column, etc.). The spec explains why; if you disagree with the rationale, open an issue.
3. **Concurrency tests are not optional.** Any change to a stored proc that affects send/read/delete/archive/grouping needs corresponding multi-session tests in `harness/src/test/java/io/freesidenomad/sqlmq/concurrency/`. The bar: produce a failing test that proves the bug, then the fix.

## Code style

- T-SQL: 4-space indent, uppercase keywords, lowercase identifiers, `BEGIN`/`END` on their own lines.
- Java: standard Google Java Style, enforced by `mvn verify`.

## License

By contributing, you agree your contributions will be licensed under Apache 2.0.
