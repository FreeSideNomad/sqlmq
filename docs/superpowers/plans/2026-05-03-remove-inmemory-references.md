# Plan: Remove in-memory references after V014 retirement

**Date:** 2026-05-03
**Status:** Draft (pending user review)
**Background:** Phase 9 (V014) retired the in-memory storage variant after a
two-environment bake-off showed it never beats on-disk. V014 dropped the
artifacts on existing in-memory queues and made `sqlmq.create_queue` throw
`'In-memory storage is not supported. Use @storage = ''ondisk'' (the default).'`.
The remaining dispatcher procs (`send`, `send_batch`, `[read]`, `read_grouped`,
`[delete]`, `pop`, `dlq_sweep`) were rewritten to drop the in-memory branch
entirely (a `meta` row with `storage_type = 'inmemory'` would throw at the
`storage_type IS NULL` line because V014's tear-down step deletes any such
row before recreating the procs). The interpreted helpers from V012
(`sqlmq._read_inmem`, `_read_grouped_inmem`, `_pop_inmem`, `_delete_inmem`)
and the natively compiled generators from V013 (`_gen_inmem_send_proc`,
`_gen_inmem_read_proc`, `_gen_inmem_pop_proc`, `_gen_inmem_read_grouped_proc`)
were left as unreachable legacy. The CHECK constraint on
`sqlmq.meta.storage_type` was left allowing both `'ondisk'` and `'inmemory'`.

Many references to the retired variant remain across migrations, Java code,
tests, docs and research. This plan classifies each as REMOVE / UPDATE /
KEEP-historical / VERIFY and proposes an execution order. Total references
across the repo: **~445 lines across 32 files** (excluding bench-results
data files which are evidence, not references in the editorial sense).

## Files to REMOVE / UPDATE

### Migrations (sql/migrations/)

#### V015__cleanup_dead_inmemory_helpers.sql (NEW — to be added)

V014 explicitly left the unreachable interpreted/native generators in place
"because dropping them would add migration noise without operational
benefit." After the V014 dust has settled and the spec is being cleaned up
for v1, that argument inverts: the dead helpers are themselves migration
noise and a recurring grep hit. Recommend adding V015 to drop:

  - `sqlmq._read_inmem`
  - `sqlmq._read_grouped_inmem`
  - `sqlmq._pop_inmem`
  - `sqlmq._delete_inmem`
  - `sqlmq._gen_inmem_send_proc`
  - `sqlmq._gen_inmem_read_proc`
  - `sqlmq._gen_inmem_pop_proc`
  - `sqlmq._gen_inmem_read_grouped_proc`

…each with `DROP PROCEDURE IF EXISTS sqlmq.<name>`. Header comment should
reference V014 and explain that the helpers are unreachable post-V014 and
removed to keep the catalog clean.

**Optional in same migration — narrow the CHECK constraint.** V014 left
`CK_sqlmq_meta_storage_type` allowing `'inmemory'` "for migration-history
coherence." Now that V014 has hard-deleted any `'inmemory'` rows, the
constraint can be safely tightened:

```sql
ALTER TABLE sqlmq.meta DROP CONSTRAINT CK_sqlmq_meta_storage_type;
ALTER TABLE sqlmq.meta ADD CONSTRAINT CK_sqlmq_meta_storage_type
    CHECK (storage_type IN ('ondisk'));
```

**Pros:** the schema becomes self-documenting (no "what is `'inmemory'`?"
question for future readers); defense-in-depth at the table layer in
addition to the proc-level rejection.

**Cons:** if any pre-V014 install never ran V014 successfully (it's in
production with `storage_type = 'inmemory'` rows still present), the
ALTER will fail. Mitigation: the V015 migration can pre-clean by
`DELETE FROM sqlmq.meta WHERE storage_type = 'inmemory'` first — but that
silently destroys data. Recommend FLAG FOR USER DECISION whether to do
this or leave the CHECK constraint alone. Default action: leave it alone
if V015 is purely about dropping helpers.

**File to add:** `sql/migrations/V015__cleanup_dead_inmemory_helpers.sql`
(no `.conf` file needed — interpreted DDL only, no memory-optimized DDL,
so it can run inside a transactional Flyway migration).

### Application-time SQL — already-applied earlier migrations

These contain `inmemory` references but **must remain unchanged** because
they're part of the applied migration history. The V014 retirement
correctly handled the run-time behavior. Do NOT edit any of:

  - `V001__core_schema.sql` (CHECK constraint mentions `inmemory`)
  - `V002__create_drop_queue_procs.sql`
  - `V003__create_queue_hardening.sql`
  - `V004__send_procs.sql`
  - `V005__read_procs.sql`
  - `V006__delete_archive_pop_procs.sql`
  - `V008__purge_dlq_sweep_procs.sql`
  - `V009__read_grouped_procs.sql`
  - `V010__send_serialize_per_queue.sql`
  - `V011__inmemory_filegroup.sql` (+ `.sql.conf`)
  - `V012__inmemory_storage.sql` (+ `.sql.conf`)
  - `V013__inmemory_native_read.sql` (+ `.sql.conf`)
  - `V014__retire_inmemory.sql` (+ `.sql.conf`)

This is true even though V001's `CHECK (storage_type IN ('ondisk', 'inmemory'))`
"looks wrong" to a fresh reader — Flyway tracks applied scripts by checksum
and editing them would either fail validation or create silent drift.

### Java production code (harness/src/main/)

#### `harness/src/main/java/io/freesidenomad/sqlmq/client/SqlmqClient.java`

`createQueue(String name, String storage, ...)` (line 21) keeps the
`storage` parameter even though only `"ondisk"` is now valid. **No
in-memory references in the file**, but the parameter is now load-bearing
only as a passthrough that always equals `"ondisk"` in product code (the
`inmemoryStorageRejected` test still passes `"inmemory"` to assert
rejection, so the param must stay).

**Recommendation: VERIFY — leave the parameter in place.** The signature
is part of the public Java client surface. Removing the parameter would
be a breaking API change. The CLI and tests already only pass `"ondisk"`
in the happy path. Cost of keeping it is zero (one extra string arg);
benefit of removing it is purely cosmetic. If the user wants a tighter
client, an overload `createQueue(String name, boolean grouped, String
payloadType, Integer maxDeliveryCount)` could be added that hardcodes
`"ondisk"` — but that's a separate ergonomics decision, not part of
in-memory removal.

#### `harness/src/main/java/io/freesidenomad/sqlmq/bench/BenchProfiles.java`

- **Line 15** — comment block mentions "ondisk and inmemory variants."
  **UPDATE:** rewrite as "the on-disk variant" (single, not "both").

#### `harness/src/main/java/io/freesidenomad/sqlmq/bench/BenchReporter.java`

The reporter has dead branches that conditionally emit a "ondisk vs
inmemory ratios" table only when both variants are present in results.
After V014 the second variant is impossible, so the conditional is
always false.

- **Lines 135-157** — the `appendOndiskInmemoryRatios` block (and its
  surrounding `byProfile` map). **REMOVE** entirely. The table will never
  appear; keeping it is dead code.
- **Lines 182-188** — column header for "ratio (im/od)" in
  `appendScanSection`. **REMOVE** the header insertion; remove the
  separator append; remove the `imTps` extraction and ratio computation
  at lines 219-220 and 226-233. Result: the consumer-scan table just
  shows "consumers | ondisk TPS" with no ratio column.
- **Line 220** — `if ("inmemory".equals(stor)) imTps = t;` becomes
  unreachable after the above. **REMOVE.**

After cleanup, `BenchReporter` still contains a `storages` set with
size 1 in practice — the loop machinery is fine to keep as-is so adding
a future second storage variant remains a one-line MethodSource change.

#### `harness/src/main/java/io/freesidenomad/sqlmq/bench/SqlmqBenchCli.java`

- **Lines 155-158** — the `--storage` option `description` mentions
  "(in-memory variant retired)." **UPDATE:** simplify to "Storage variant
  to test. Only 'ondisk' is supported. Default: ondisk." Remove the
  retirement parenthetical (we don't bake "previously retired" into help
  text post-cleanup; the README explains it).
- **Lines 441-463** — `resolveStorage()` accepts `"inmemory"` / `"both"`
  inputs and warns. **REMOVE the coercion branch.** Replace with a
  hard parameter rejection (the `default ->` branch already does this
  for unknown values). Specifically, the `case "inmemory", "both" -> {...
  coercing to 'ondisk' }` block becomes part of the `default ->` branch.
  Resulting behavior: `--storage inmemory` exits with `"Unknown --storage
  value 'inmemory'. Only 'ondisk' is supported."` This is consistent with
  V014 having rejected the variant for ~12 months by the time the next
  release ships.

  **Operator-script-compat consideration:** any bench script someone
  still has lying around with `--storage inmemory both` will fail loudly.
  That's correct behavior: silent coercion hides operator intent. The
  V014 retirement was the warning shot; V015 is the removal.

  **VERIFY:** ask user whether the soft-coerce-with-warning ergonomics
  should be kept for one more release. Default recommendation: REMOVE.

### Java test code (harness/src/test/)

#### `harness/src/test/java/io/freesidenomad/sqlmq/support/StorageVariants.java`

Already returns only `"ondisk"`. The class is intentionally kept as
"a structural anchor for future variants." JavaDoc on lines 11-14
mentions V014 retirement.

**Recommendation: KEEP.** The author note is correct — the parameterization
plumbing (MethodSource indirection, `Stream<Arguments>`) costs nothing and
re-enabling a second variant is a one-line edit. The class also keeps the
test-code shape symmetric with `StorageVariants`-using tests in `concurrency/`
that would otherwise have to be rewritten when reintroduced. JavaDoc can be
trimmed slightly:

- **UPDATE:** shorten the V014 reference to one sentence (don't describe the
  bake-off in JavaDoc — link to the V014 migration if needed).

#### `harness/src/test/java/io/freesidenomad/sqlmq/correctness/CreateDropQueueTest.java`

- **Lines 50-58** — `inmemoryStorageRejected` test: KEEP the test (it asserts
  the V014 rejection is in place). **UPDATE** the comment on line 52 to
  drop the "V014" version reference once V014 is N migrations in the past
  — for now, leave the V014 attribution since it's recent.

#### `harness/src/test/java/io/freesidenomad/sqlmq/concurrency/NoLossNoDoubleDeliveryTest.java`

- **Line 24** — comment "V014: in-memory storage retired — only on-disk
  runs now." **UPDATE:** simplify to "Only on-disk runs (V014)" or remove
  entirely. Method source already only emits `"ondisk"` so the comment is
  redundant with the code.

#### `harness/src/test/java/io/freesidenomad/sqlmq/concurrency/GroupedFifoInvariantTest.java`

- **Line 134-156** — `@Disabled` annotation on
  `perGroupMsgIdOrderingIsStrictWithSingleMessageReads` documents the
  Java-clock-jitter false-positive issue. **KEEP.** The rationale is
  independent of in-memory retirement (it's about Java-side timing
  measurement, not about storage). The test still parameterizes over
  `StorageVariants#all` which is fine.

#### `harness/src/test/java/io/freesidenomad/sqlmq/benchmark/BakeOffRunner.java`

- **Lines 51-52** — explicit `for (var storage : List.of("ondisk"))` with
  a V014 comment. **UPDATE:** replace the `for` loop with direct
  invocation: `var storage = "ondisk";` (one less indentation level,
  signals "this is permanent, not a parameterization"). Remove the V014
  comment.
- **Line 148** — comment "V014: in-memory storage retired — no side-by-side
  ratios needed." **REMOVE** the comment along with any nearby
  retirement-explanation prose; the absence of the ratio code speaks for
  itself.

#### `harness/src/test/java/io/freesidenomad/sqlmq/benchmark/BenchmarkProfiles.java`

- **Lines 11-19** — comment block has "ondisk and inmemory variants" and
  "(ondisk, inmemory) within each profile." **UPDATE:** rewrite to
  describe only the ondisk profile sizing. Specifically, drop the
  "for both ondisk and inmemory" qualifier on line 13 and the `"directly
  comparable across (ondisk, inmemory)"` claim on lines 18-19.

### Documentation

#### `README.md`

- **Line 19** — the "What it isn't (in v1)" bullet about in-memory.
  **VERIFY then KEEP-AS-IS or LIGHTLY-EDIT.** This is the user-facing
  explanation of the retired variant. Removing it would make readers
  who saw earlier versions of the spec wonder where in-memory went.
  Recommendation: keep the bullet but consider trimming the technical
  detail (memory-optimized + READPAST + 41302) to a single sentence
  that links to V014 for the deep dive. Default: leave as-is — the
  technical detail is what makes the bullet useful (reader doesn't
  have to follow a link to know why).
- **Lines 30, 84** — both reference `--storage` defaulting to ondisk
  "since V014" / "as of V014." **UPDATE** to drop the V014 marker once
  the cleanup ships; they're unhelpful version anchors. Replace with
  "the only supported value." Default: keep V014 marker for one more
  release, then strip in the next cleanup pass.

#### `CLAUDE.md`

- **Lines 23, 30, 40** — three load-bearing references to the in-memory
  variant in the project's permanent guidance file. **UPDATE all three:**
  - **Line 23 ("In-memory option" bullet):** rewrite as "**On-disk only.**
    The in-memory (Hekaton) variant was investigated and retired in V014
    (see V014 header for the bake-off rationale). New work should not
    re-introduce memory-optimized tables without re-running the bake-off."
  - **Line 30 ("Queue lifecycle" — `create_unlogged` analogue):** drop
    "in-memory or" — leave just `"SIMPLE recovery DB"` as the closest
    analogue.
  - **Line 40 ("Tests must run against both"):** rewrite as "Tests
    parameterize over `StorageVariants#all()`, currently `{ondisk}` only
    after V014. Adding a future variant is a one-line change there."

#### `docs/superpowers/specs/2026-05-02-sqlmq-port-from-pgmq-design.md`

This is the **design spec**, with a "Retired in v1" intro section (lines
8-22) that already explains the situation. Body text below still describes
the in-memory variant in detail at numerous points (lines 103, 137, 146-151,
175, 198, 202, 206-208, 212-216, 260-285, 341, 368, 411, 438-440, 457).

**Recommendation:** the existing intro section is exactly the right
abstraction — it warns the reader that in-memory references downstream
are historical. **KEEP body references unchanged.** Updating each one
inline ("the in-memory variant *which was retired in V014*") would add
noise without informational gain — the reader has already been told.

The one exception: the **§"Per-queue specialization (in-memory only)"**
header at line 212 and the **§"In-memory dequeue — natively compiled,
optimistic, retry on conflict"** header at line 260 are stand-alone
section headings that a casual reader might land on without seeing the
intro. **UPDATE each header** by appending `(retired in v1 — see "Retired
in v1" above)` to the heading text. Keeps them findable but signals
"don't implement this." Body text stays.

The bake-off section (lines 405-440) also needs the same treatment:
**UPDATE the §10 "Bake-off" heading** with the same "(retired)" suffix.

#### `docs/superpowers/plans/2026-05-02-sqlmq-v1-implementation.md`

This is the **implementation plan** that drove V001-V014. It's a
historical artifact at this point — Phase 9 retrospective at lines
3700-3706 already documents the retirement. The plan has many `inmemory`
references (lines 5, 7, 37, 613, 804, 810, 823-828, 988-992, 1049, 1054,
1144, 1195, 1429, 1610, 1614, 1686, 1942, 1945, 2388, 2833, 2963,
3235-3415, 3496, 3634, 3656, 3680, 3687, 3700-3706).

**Recommendation: KEEP entirely as-is.** This is the work plan that
*was executed*. Editing it post-execution makes future archeology harder.
The Phase 9 retrospective at the end already provides the closure note.

#### `research/`

- `research/sqlserver-patterns.md` — citations of MS Learn pages on
  memory-optimized tables and natively compiled procs. Section §3
  ("Memory-optimized + natively compiled") is the entire Hekaton write-up.
  **KEEP.** This is research source material; the conclusion (in V014
  and the spec intro) was that the pattern doesn't fit this workload, but
  the underlying research is still valid reference for SQL Server
  practitioners.
- `research/pgmq-surface.md` — line 741 maps PG `UNLOGGED` to SQL Server
  `MEMORY_OPTIMIZED` and on line 772 says "probably skip." **KEEP.**
  Already cautions against this path.
- `research/sqlserver-longpoll.md` — multiple references discussing
  why long-polling can't work in the in-memory variant. **VERIFY.**
  These references are now moot (no in-memory variant exists). However,
  the research file's conclusion (don't ship `read_with_poll`) is still
  correct; rewriting it to scrub in-memory mentions would cost effort
  for low value. Recommendation: leave the file as historical research,
  same treatment as `pgmq-surface.md`.

### Bench results (`bench-results/`)

All four directories (`scan-local-rosetta/`, `scan-vm-native/`,
`scan-vm-native-v2-native-read/`, `v1-baseline/`) contain raw bench
data with `inmemory` columns and rows. **KEEP entirely.** These are
the evidence behind V014. Removing them would erase the basis for
the retirement decision.

## Files to KEEP (historical / load-bearing)

Already enumerated inline above. Summary:

- **`sql/migrations/V001`-`V014`** — all applied, do not edit (Flyway
  checksum + project policy: forward-only, no edits to applied scripts)
- **`bench-results/scan-local-rosetta/`**, **`bench-results/scan-vm-native/`**,
  **`bench-results/scan-vm-native-v2-native-read/`**, **`bench-results/v1-baseline/`**
  — bake-off evidence
- **`docs/superpowers/specs/2026-05-02-sqlmq-port-from-pgmq-design.md`**
  intro "Retired in v1" header + body (only headings get a "retired"
  suffix per above)
- **`docs/superpowers/plans/2026-05-02-sqlmq-v1-implementation.md`** —
  the executed work plan, untouched
- **`research/sqlserver-patterns.md`**, **`research/pgmq-surface.md`** —
  background research
- **`@Disabled` annotation on `perGroupMsgIdOrderingIsStrictWithSingleMessageReads`**
  — its rationale is Java-clock jitter, not in-memory retirement

## Files to VERIFY (decisions for the user)

1. **V015 CHECK constraint narrowing.** Should `CK_sqlmq_meta_storage_type`
   be tightened from `IN ('ondisk', 'inmemory')` to `IN ('ondisk')`?
   - Pros: schema self-documents
   - Cons: may need a `DELETE FROM meta WHERE storage_type = 'inmemory'`
     pre-step that silently destroys data on installations that never
     completed V014
   - Default recommendation: **leave it alone**, V015 only drops dead
     procs.

2. **`SqlmqClient.createQueue(..., String storage, ...)` parameter.** The
   parameter is no longer load-bearing. Should an overload be added that
   omits it (always passes `"ondisk"`)? Or should the param be removed
   entirely (breaking change)?
   - Default recommendation: **leave the parameter alone.** Tests still
     pass `"inmemory"` to verify rejection.

3. **CLI `--storage` coercion of `inmemory`/`both`.** Currently soft-coerces
   with a warning. Should it hard-reject, or keep the soft path one more
   release?
   - Default recommendation: **hard-reject** (consistent with V014 having
     been the warning shot).

4. **README.md line 19 — depth of explanation.** Keep the technical
   detail (READPAST / 41302 / SNAPSHOT) inline, or trim and link to V014?
   - Default recommendation: **keep inline.** Helps readers who don't
     follow links.

5. **Spec headings "(retired)" suffix.** Three section headings call out
   in-memory specifically (`Per-queue specialization (in-memory only)`,
   `In-memory dequeue ...`, `§10 Bake-off`). Append `(retired in v1)` to
   each heading?
   - Default recommendation: **yes, append.** Keeps the section findable
     by grep but signals "don't implement this" to anyone who lands
     directly via TOC.

## Removal order (suggested execution sequence)

Each step ends with a separate commit; CI must be green before the next
step starts.

1. **V015 migration first.** Add `sql/migrations/V015__cleanup_dead_inmemory_helpers.sql`
   with the `DROP PROCEDURE IF EXISTS` block. Run the test suite — all
   tests should still pass because none call the dropped procs (proven
   by V014 being green for the dispatchers and by `rg` finding zero
   callers of `_read_inmem` / `_pop_inmem` / `_delete_inmem` /
   `_read_grouped_inmem` / `_gen_inmem_*` outside of V012/V013 themselves).

2. **Java production code cleanup.** In one commit:
   - `BenchReporter.java`: remove dead ratio branches and `imTps`
     references.
   - `SqlmqBenchCli.java`: remove soft-coerce branch from `resolveStorage()`,
     simplify `--storage` description.
   - `BenchProfiles.java`: trim "ondisk and inmemory" comment.

   Run `mvn -B test` against a fresh container — should pass identically.

3. **Java test code cleanup.** In one commit:
   - `BakeOffRunner.java`: replace `for (var storage : List.of("ondisk"))`
     with direct `var storage = "ondisk"`; remove V014 comments.
   - `BenchmarkProfiles.java`: trim comment.
   - `NoLossNoDoubleDeliveryTest.java`: trim or remove V014 comment.
   - `CreateDropQueueTest.java`: leave as-is (test still asserts the
     rejection contract).
   - `StorageVariants.java`: trim JavaDoc (one-sentence V014 ref).

   Run `mvn -B test`. Pass.

4. **Documentation cleanup.** In one commit:
   - `CLAUDE.md`: rewrite three bullets per above.
   - Spec: append `(retired in v1)` to three section headings.
   - README.md: optional minor trim of the V014 line markers.

   No tests to run; review by reading.

5. **(Optional, separate decision) CHECK constraint narrowing in V016**
   if the user opts in. Pending verification on operational risk.

## Estimated scope

- **~7 files modified** across Java production + tests + docs
- **1 new migration** (V015) drops 8 unreachable procs
- **0 files deleted** (StorageVariants kept as anchor; ratio computation
  inlined-out of BenchReporter rather than moving file)
- **Test impact:** zero new test failures expected. Existing
  `inmemoryStorageRejected` test continues to assert the V014 rejection
  contract.
- **Estimated wall time for execution:** ~2-3 hours including running
  the full Testcontainers suite three times (once per code-touching
  commit). Each suite run is ~10-15 minutes against a clean container.
- **Risk:** very low. V015 dropping the unreachable procs is the only
  schema-touching change, and there are zero callers of the dropped
  procs anywhere in the repo (verified via `rg` of the harness Java
  code and remaining migrations).
