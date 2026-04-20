# Contributing

An Elixir Ecto adapter backed by Holographic Reduced Representations
(HRR), persisted to SQLite.  `LIKE`/`ILIKE` predicates route to HRR
cosine-similarity probes instead of string matching, inner joins support
both exact hash and HRR semantic strategies, and nested transactions use
SQLite savepoints.  The C++ NIF layer (`Taskweft.NIF`) is compiled via
`elixir_make`; `exqlite` owns the SQLite connection.

Built strictly red-green-refactor: every feature is driven by failing
PropCheck properties, committed when green, then any cleanup is done
with the properties still green.  The suite currently covers 163
properties across Storage, Query, transactions, joins, and aggregates.

## Guiding principles

- **RED first, always.** Before writing implementation code, write a
  property (or unit test) that fails at runtime.  Validate that it
  fails for the right reason — mutation-test it by briefly breaking the
  implementation if the failure message is ambiguous.
- **Narrow the slice.** Each cycle is one public behaviour: one
  Storage API call, one WHERE strategy, one aggregate function.  If
  turning a property green requires touching two independent paths,
  split it into two cycles.
- **Error tuples, not exceptions.** Functions return `{:ok, value}` /
  `{:error, reason}` at every boundary.  `raise` is reserved for
  programmer errors that represent bugs in the calling code, not
  runtime conditions.  NIF boundary `rescue` blocks are the one
  accepted exception — they prevent a NIF crash from crashing the
  GenServer, and they must return a typed fallback (`nil`, `[]`, `:ok`)
  that callers can distinguish.
- **Commit every green.** One commit per cycle (or tightly-paired
  cycle).  The TDD arc should be legible in `git log`.  Messages use
  sentence case; do not use Conventional Commits prefixes (`feat:`,
  `fix:`, `chore:`, etc.).
- **PropCheck, not mocks.** Generators produce the inputs; properties
  express the invariant.  If a property needs a generator that is hard
  to write, that is a signal the API surface is too wide, not that a
  mock is needed.
- **Ecto is optional.** All behaviour registrations are guarded by
  `Code.ensure_loaded?`.  The library must compile and the NIF tests
  must pass in a project that does not depend on Ecto.

## Design notes

### The phases / bytes NIF type boundary

Every HRR NIF has a strict input/output type:

| NIF                     | Input        | Output  |
|-------------------------|--------------|---------|
| `hrr_encode_atom/2`     | string, dim  | phases  |
| `hrr_encode_text/2`     | string, dim  | phases  |
| `hrr_phases_to_bytes/1` | phases       | bytes   |
| `hrr_bytes_to_phases/2` | bytes, 0     | phases  |
| `hrr_bind/2`            | bytes, bytes | bytes   |
| `hrr_unbind/2`          | bytes, bytes | bytes   |
| `hrr_bundle/1`          | [bytes]      | bytes   |
| `hrr_similarity/2`      | phases, phases | float |

Phases are lists of floats (radians); bytes are Erlang binaries
(little-endian float64 arrays).  `hrr_similarity` operates on phases
only — passing bytes silently produces garbage similarity scores.
`hrr_bundle` handles the bytes→phases→average→bytes round-trip
internally.  Every call site must convert explicitly; no helper should
hide the conversion to keep the boundary visible.

### Deferred bundle rebuild inside transactions

`hrr_bundles` holds the superposition of all `record_vector`s for a
source and is the backing store for `COUNT(*)` and `probe_*` operations.
Rebuilding it on every insert/delete inside a transaction would be
incorrect — the bundle would reflect a partial commit.  Instead:

- Insert and delete calls inside a transaction mark the source in a
  `dirty` `MapSet` in GenServer state but skip `rebuild_bundle`.
- At outermost `COMMIT`, the dirty set is drained and each source is
  rebuilt after the SQLite `COMMIT` returns.
- Nested transactions (savepoints) accumulate the same dirty set;
  only the outermost commit triggers rebuilds.
- A savepoint `ROLLBACK TO` does not clear the dirty set — the
  outer transaction may still commit, so dirty sources remain tracked.

### COUNT(*) O(1) fast path

`Repo.aggregate(:count)` without a WHERE clause or joins reads
`hrr_bundles.record_count` rather than scanning `hrr_records`.
`record_count` is kept accurate by the bundle rebuild: it is set to
`length(record_vectors)` on every rebuild and deleted when the source
is empty.  The fast path is taken in `Query.execute` only when
`joins == [] and wheres == []`; any filter or join falls through to a
counted full scan.

### Nested transactions via savepoints

SQLite does not support `BEGIN` inside an open transaction.  The
adapter uses `txn_depth` in GenServer state to track nesting:

- Depth 0 → 1: `BEGIN`
- Depth N → N+1: `SAVEPOINT sp{N}`
- Depth 1 → 0 commit: `COMMIT`, then rebuild dirty bundles
- Depth N → N-1 commit: `RELEASE sp{N-1}`
- Depth 1 → 0 rollback: `ROLLBACK`
- Depth N → N-1 rollback: `ROLLBACK TO sp{N-1}`
- Depth 0 commit or rollback: `{:error, :not_in_transaction}`

`Ecto.Repo.transaction/2` calls are transparently composable — an
inner `Repo.transaction` inside an outer one promotes to a savepoint
without any change at the call site.

### Conditional Ecto behaviour registration

```elixir
if Code.ensure_loaded?(Ecto.Adapter) do
  @behaviour Ecto.Adapter
end
```

This is evaluated at compile time.  If Ecto is not in the dependency
graph, the `@behaviour` attribute is never set and no callbacks are
required.  All four adapter behaviours (`Ecto.Adapter`,
`Ecto.Adapter.Schema`, `Ecto.Adapter.Queryable`,
`Ecto.Adapter.Transaction`) are guarded this way.  Downstream projects
that mix in Ecto get full adapter compliance; projects that use only
the Storage and Query APIs directly do not pay the Ecto compile-time
cost.
