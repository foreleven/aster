# Artifact and Context Synchronization Design

Status: proposed

## Summary

Artifacts are durable work products produced while a Goal is being pursued. An Artifact has a stable logical identity, immutable versions, provenance, and one Goal owner. The Artifact domain is the source of truth for artifact identity, versioning, receipts, and synchronization state.

Artifacts may be projected into an external or integration-owned Context. The target is addressed by an Actor path. The synchronization command is defined in `@aster/core`; a target Actor implements that command. Core never imports Notion, Lark, a storage SDK, or any other target implementation.

The dependency direction is:

```text
@aster/core artifact protocol
        ↑
target Actor implementation (Notion, Lark, local files, ...)
        ↑
runtime and integration assembly
```

An Artifact is not a transcript entry, an audit log, a memory observation, or a Context snapshot. Pi remains the owner of Goal and Task conversations. Memory indexes Artifact text after durable publication. Contexts receive explicit synchronization commands or provide read-only knowledge evidence.

## Current implementation decision

The first implementation does not implement Artifact synchronization. It gives every Goal a durable local working directory and connects that directory to the Goal's pi-durable conversation environment. The Goal can use pi-durable's file tools to read, write, and edit files, so local work products exist as ordinary files before they become a separately managed Artifact domain.

The local runtime stores workspaces below `<durable-root>/workspaces`. A Goal uses `goals/<goal-slug>` and its internal Tasks use that same Goal directory by default. The directory is an ordinary working directory; the runtime does not impose `artifacts/`, `scratch/`, or `tasks/` subdirectories. pi-durable exposes read, write, and edit tools with that workspace as `cwd`, while shell execution is disabled. The durable agent stores the workspace-relative `cwd`, so recovery reopens the same path.

This phase deliberately keeps the surface small:

- Goal-owned workspace directories are persistent across turns and process restarts.
- The default work directory is derived from the stable Goal identity and the configured durable root.
- The Goal conversation receives file tools; the read-only relevance gate does not.
- Tasks use the owning Goal workspace by default; an isolated Task directory is an explicit execution policy, not a required layout.
- No Notion, Context synchronization, ArtifactSyncActor, watcher, or automatic file-to-version capture is added in this phase.

The later Artifact phase can index or promote these files without changing the initial workspace contract.

The detailed local Context design is documented in [workspace-context-design.md](workspace-context-design.md). It defines the relationship between the Goal file environment, the read-only WorkspaceContextActor, Context queries, and future explicit Artifact promotion.

## Local workspace model

The current local layout is:

```text
<config.durable.root>/
  conversations/                 Pi conversation storage
  workspaces/
    goals/
      <goal-slug>/                one ordinary working directory
```

The durable root remains the storage and process-lease root. A Goal workspace is a user-visible working directory beneath that root; it is not a second persistence authority. Conversation entries and Goal state continue to use their existing stores.

The workspace identity is supplied to the Agent harness as an opaque stable identifier. The local environment factory owned by infrastructure maps it to a directory below the durable root. It is never supplied by the model and is never persisted as a live filesystem handle. Relative paths in tools are resolved against the Goal environment's `cwd`.

The first local environment uses an infrastructure-provided pi-durable `ExecutionEnv` with the Goal workspace as `cwd`, together with the explicitly selected file tools (`read`, `write`, and `edit`; `bash` is a separate capability decision). The Agent package does not construct local filesystem adapters. Setting `cwd` alone is not a security sandbox; root enforcement is deferred to a later pi-durable extension or stronger process/container boundary.

The Goal's tools should describe the workspace using relative paths. They should not expose the absolute durable root to the model. A public Goal view may expose a redacted workspace label and selected relative paths, but not credentials, process handles, or arbitrary host paths.

## Capabilities of the local workspace phase

The local workspace provides a useful working surface before full Artifact management exists:

1. A Goal can draft reports, plans, notes, tables, patches, and other files across multiple turns.
2. A Task can create intermediate files and return relative file references to its Goal.
3. A user can open and edit the same files in an editor or inspect them with normal local tools.
4. Later Goal turns can read the files instead of relying on a long conversation transcript.
5. A repository can be used as a Task workspace, allowing tests, diffs, and generated documentation to coexist with the Task execution.
6. A future Artifact layer can promote a selected file into an immutable Artifact version using its path, content hash, and Goal/Task provenance.

The file is the source of truth during this phase. There is no automatic version history, external projection, or Context event for every filesystem write. A future explicit import or promotion operation will establish the Artifact boundary.

## pi-durable integration contract

Core's `agent/runtime` owns the thin pi-durable session boundary. The harness opens one durable conversation per owner and accepts an environment factory in pi-durable. The Goal execution path should supply:

```text
owner: Goal path
cwd: Goal workspace directory
environment: host-provided ExecutionEnv
tools: selected pi-durable file tools
```

The environment factory must be scoped to the conversation and release any watchers or child processes with the conversation scope. The same owner and workspace identity must be used when recovering a pending native submission; a recovered submission cannot silently switch to another directory. Isolation is intentionally deferred to a later pi-durable extension.

The Goal Agent and its local tools share the same Goal-owned state writer for business mutations. File operations are an agent execution capability and do not mutate `GoalSnapshot` directly. A later promotion operation must enter the Artifact owner and persist its result before acknowledging the caller.

Task workspace selection is explicit in the Task execution input. An internal Task uses its reply Goal's workspace by default and may receive a derived subdirectory only when an execution policy requests isolation. External executor workspaces remain owned by their executor adapter and are not implicitly merged with the Goal workspace.

## Workspace configuration

The first configuration only needs a durable root and optional workspace policy. It does not expose Actor paths or external sync targets:

```yaml
config:
  durable:
    root: ./.aster

goals:
  - slug: project
    workspace:
      mode: read-write
      shell: disabled
      maxFileBytes: 10485760
```

The default workspace is derived as `workspaces/goals/<slug>` below the resolved durable root. No child directories are created or prescribed by the workspace contract. `shell: disabled` is the recommended first default. Enabling `bash` requires a separate capability, command allowlist or stronger sandbox implementation; it must not be implied by enabling file read/write tools.

The workspace policy is module configuration decoded by the host and supplied to the Goal execution Layer. It is not Goal business state, and it does not introduce a new Context or Actor registry.

## Implementation plan for the first phase

### 1. Define workspace settings and path resolution

Add a typed infrastructure settings module for the workspace root and policy. Resolve paths with Effect `Config`, `Path`, and `FileSystem`. Derive one stable directory per Goal. Create the directory lazily when a Goal first needs a file environment. Optional Task isolation can be added later without changing the Goal workspace contract.

### 2. Add a workspace environment extension

Implement the isolation policy as a pi-durable extension around the injected `ExecutionEnv`. It must:

- canonicalize and validate every path;
- reject paths outside the assigned root;
- reject symlink escapes;
- enforce file and directory size limits;
- use the current Effect cancellation signal for file and process operations;
- close watchers and terminate child processes through the owning Scope;
- report typed file and execution errors to the Agent tool boundary.

### 3. Connect Goal conversations to the environment

Extend the Agent harness options only at the adapter seam. Goal construction supplies the workspace `cwd`, registers the selected pi-durable tools, and preserves the same environment policy during recovery. The relevance gate continues to run without write-capable tools.

### 4. Decide Task workspace inheritance

Internal Tasks use a stable derived child directory only when the Task requires local files. The Task admission retains the relative workspace identity, not an absolute host path. Goal feedback includes relative output paths and concise summaries; it does not copy large file contents into Goal state.

### 5. Add focused tests

Use temporary roots and fake models. Test path traversal, symlink escape, read/write/edit behavior, disabled shell behavior, cancellation cleanup, Goal restart with the same `cwd`, independent Task directories, and no leakage of absolute paths into model-facing prompts. Test that normal Goal state persistence is unchanged by file operations.

## Later Artifact promotion

Once the local workspace phase is stable, add an explicit `artifact_publish` operation that accepts a relative workspace path. It reads the file through the restricted environment or content store, records a hash and provenance, and creates an immutable Artifact version. The operation should be explicit first; file watching and automatic capture can be evaluated after the promotion contract has real usage.

The later synchronization design remains available after promotion:

```text
Goal workspace file
    ↓ explicit artifact_publish
Artifact version
    ↓ optional ArtifactSyncCommand
target Actor / Context / external system
```

## Goals

- Preserve every accepted work product with a stable identity and immutable version history.
- Make Task and Goal outputs addressable without copying large bodies into Goal or Task snapshots.
- Project an Artifact to one or more target Actors through a small, typed Core protocol.
- Let a target Actor implement external mapping and external-operation recovery without a Core-to-integration dependency.
- Use target Contexts as knowledge sources while retaining provenance, source boundaries, and loop protection.
- Make accepted publication, synchronization, retry, and recovery observable and testable.
- Support a local implementation first and external integrations such as Notion later.

## Non-goals

- Replacing Pi as the Goal or Task message store.
- Treating every model response, tool call, log line, or Context event as an Artifact.
- Making an external service the canonical Artifact database.
- Introducing a universal untyped command registry or a generic event bus.
- Automatically overwriting an Artifact when a target Context is edited.
- Adding cross-Goal sharing and permissions in the first implementation.
- Fetching arbitrary external URLs merely because an Artifact references them.

## Domain model

### Logical Artifact

An Artifact is the durable logical object owned by one Goal. Its identity survives title changes, version changes, Goal archival, and target synchronization.

```text
Artifact
  id: ArtifactId
  owner: GoalPath
  kind: ArtifactKind
  title: string
  status: active | archived
  latestVersion: number
  tags: string[]
  createdAt: ISO timestamp
  updatedAt: ISO timestamp
```

Recommended initial kinds are `document`, `report`, `dataset`, `image`, `code`, `link`, and `note`. The Core schema should use a closed union for protocol validation while allowing a later additive kind change.

### Immutable ArtifactVersion

Every content change appends a version. A version is never edited in place.

```text
ArtifactVersion
  artifactId: ArtifactId
  version: positive integer
  content: ArtifactContentRef
  sha256: string
  size: non-negative integer
  mimeType: string
  summary?: string
  source: ArtifactSource
  derivedFrom: ArtifactRef[]
  createdAt: ISO timestamp
```

`ArtifactContentRef` is one of:

- `InlineText`: small UTF-8 text with a MIME type;
- `InlineJson`: bounded JSON data;
- `Blob`: an opaque content-store key, hash, size, and MIME type;
- `External`: a URL explicitly supplied by a caller.

Core never exposes local filesystem paths, provider handles, credentials, or temporary upload paths in a public Artifact view.

### Provenance

The primary source of a version is stable data, never a live Actor reference:

```text
GoalTurn    { goalPath, inputId }
Task        { taskPath, roundId, requestId }
UserUpload  { goalPath, inputId }
Imported    { sourceDescription }
```

`derivedFrom` links a version to earlier Artifact versions. The relation is immutable and supports audit, regeneration, and knowledge explanations.

### References and views

An `ArtifactRef` contains only an ID, title, version, status, and optional summary. Goal and Task public views may resolve references through `ArtifactCatalog`; they do not duplicate Artifact content or version history in their snapshots.

The Artifact catalog is authoritative. Goal and Task snapshots may keep bounded projection hints later, but those hints are repairable and never become a second Artifact authority.

## Ownership and package boundaries

### `packages/core/src/artifacts`

Core owns:

- Artifact schemas and public views;
- Artifact state transitions and invariants;
- Core synchronization commands and replies;
- Artifact and synchronization receipts;
- target ActorPath configuration contracts;
- query operations and provenance rules;
- the `ArtifactContentStore` capability contract;
- recovery and exact-retry policy.

Core does not import integration packages, Notion SDK types, filesystem implementations, or object-store clients.

### `packages/infra/src/artifacts`

Infra provides concrete content storage and local settings. A local adapter stores content by hash under a configured root using Effect `FileSystem` and `Path`. It owns temporary staging, atomic rename, size limits, and cleanup of uncommitted staging data.

### `packages/integrations/src/notion`

The Notion integration implements the Core synchronization command in a target Actor. It owns Notion page and database mapping, Notion properties and blocks, rate limits, external receipts, and Notion-specific recovery. It may consume the Core `ArtifactContentStore` capability to read a Blob.

### `packages/api`

The API exposes Artifact metadata, paginated lists, version details, synchronization status, and content streaming. Binary content is streamed through a dedicated boundary; it is not embedded in JSON RPC responses.

### `apps/local`

The local app captures configuration, provides the configured integration and content-store Layers, and assembles the runtime. It does not construct Artifact state models or target Actors directly.

## Core protocol

The synchronization protocol lives in `packages/core/src/artifacts/protocol.ts` and uses Effect `Schema` and Actor commands. The target Actor imports this protocol.

```text
ArtifactSyncCommand
  requestId: CommandIdentifier
  artifact: ArtifactSyncEnvelope
  operation: upsert | append | import

ArtifactSyncReply
  Accepted  { receipt: CommandReceipt }
  Rejected  { error: ApplicationError }
  Unknown   { operationId: string }
```

`ArtifactSyncEnvelope` contains only target-independent fields:

```text
artifactId
version
ownerGoal
title
kind
tags
contentRef
sha256
source
derivedFrom
```

The protocol does not contain `NotionPageId`, Notion database properties, Lark document tokens, or any other integration vocabulary.

The Core protocol is the single contract for target Actors. A target Actor may support a subset of operations, but it must reject unsupported operations with a typed `ApplicationError`. Target capability discovery is optional for the first implementation; runtime configuration should at least validate that the configured Actor path resolves and becomes ready.

## Actor topology

The runtime registers an Artifact root before Goals and Tasks:

```text
/user/artifacts
  /<artifact-id>       ArtifactActor
  /sync                 ArtifactSyncActor
```

### ArtifactRootActor

The root owns registration, path lookup, owner queries, and child restoration. It never calls an external provider.

### ArtifactActor

One Artifact Actor owns the serialized business state for one logical Artifact:

- creation and idempotent admission;
- append-version with `expectedVersion` conflict checks;
- archive and restore;
- version metadata and provenance;
- synchronization projection records for this Artifact.

The Actor mailbox is the sole writer. The Artifact content body is stored by `ArtifactContentStore`; the Actor stores only content references and hashes.

### ArtifactSyncActor

The sync Actor owns durable delivery records and scheduling:

- accepts a synchronization request after an Artifact version is committed;
- resolves the configured target ActorPath;
- sends the Core `ArtifactSyncCommand`;
- persists target receipts before acknowledging synchronization;
- retries exact internal delivery using the original request identity;
- retains `unknown` operations for reconciliation;
- never guesses that an external operation was cancelled or absent.

The sync Actor does not know how the target Actor maps the Artifact to Notion, Lark, or another system.

## ArtifactCatalog interface

The public Core seam should remain small and deep:

```text
publish(input) -> Effect<ArtifactReceipt, ArtifactError>
list(query) -> Effect<ArtifactPage, ArtifactError>
get(id, version?) -> Effect<ArtifactView, ArtifactError>
readContent(id, version?) -> Stream<Uint8Array, ArtifactError>
archive(id) -> Effect<ArtifactReceipt, ArtifactError>
restore(id) -> Effect<ArtifactReceipt, ArtifactError>
sync(id, version, target) -> Effect<SyncReceipt, ArtifactError>
```

`publish` creates an Artifact when no `artifactId` is supplied and appends a version when an existing ID and expected version are supplied. The operation is idempotent by `requestId`; reusing an ID with a different normalized payload fails with `conflict`.

The public interface does not expose actor refs, mailbox commands, storage sessions, or external SDK clients.

## Durable publication and content storage

Publication has two durable boundaries:

1. Content is staged or written by `ArtifactContentStore`.
2. The Artifact manifest and version are committed by the Artifact Actor.

The caller receives success only after the manifest commit. If the content operation has an unknown result, the content key and hash remain recoverable. The implementation does not blindly upload again; it first reconciles by content identity.

The content-store capability is intentionally narrow:

```text
stage(stream, metadata) -> Effect<StagedContent, ContentError>
commit(staged)          -> Effect<ContentRef, ContentError>
read(contentRef)        -> Stream<Uint8Array, ContentError>
discardStaged(staged)   -> Effect<void, ContentError>
```

Committed content is immutable. Garbage collection may remove unreferenced staged content after a retention window, but it must not delete content referenced by a committed Artifact version.

## Synchronization state machine

Each Artifact version and target pair has one durable synchronization record:

```text
pending -> sending -> accepted
                   ├-> rejected
                   └-> unknown -> reconciling -> accepted | rejected | unknown
```

Rules:

- A target receives the same `requestId` for exact retries.
- A positive target receipt marks the version synchronized.
- A target rejection is retained with its typed error.
- A lost or ambiguous reply becomes `unknown`.
- `unknown` work is reconciled through the target Actor's operation lookup or receipt query.
- A new request is created only for an explicit retry of known failed work.
- Version `N+1` may be published while version `N` is still synchronizing, but the target Actor must receive version identity and may apply its own ordering policy.

## Target Actor requirements

Every target Actor that implements `ArtifactSyncCommand` must:

- validate the envelope schema at its boundary;
- validate that the target owns or is allowed to receive the Artifact;
- persist an idempotency record before acknowledging acceptance;
- commit its local projection before returning `Accepted`;
- retain an external operation ID when an external call has an uncertain result;
- reconcile the same external operation instead of submitting a replacement;
- return `Unknown` when it cannot prove the external outcome;
- keep external provider credentials and handles out of Core records and public views.

The target Actor may maintain a local Context projection. That Context is a consumer of Artifact data and is not the Artifact authority.

## Context integration

A Context that accepts Artifact synchronization implements the Core command at its Actor path. The command may update the Context owner state, append a durable Context event, or update an integration-owned projection, depending on the Context contract.

The target Actor should expose a stable actor path and, where applicable, a business Context path:

```text
actorPath   = /user/notion/knowledge
contextPath = /notion/knowledge
```

These values have different meanings. `actorPath` is the message endpoint. `contextPath` identifies the resulting business Context and its public read boundary.

Configuration points to the Actor path. The target Actor owns the mapping from a generic Artifact envelope to its Context or external representation.

The first command operations are:

- `upsert`: make the target projection represent this Artifact version;
- `append`: add a new entry without replacing an existing target object;
- `import`: explicitly create a new Aster Artifact version from target content.

The configuration stores an operation name, but the operation is still validated against the Core protocol. It is not an arbitrary shell command or untyped JSON RPC.

## Notion implementation

The Notion integration is an implementation of the target Actor contract:

```text
/user/notion
  /knowledge          NotionKnowledgeActor
  /artifacts          NotionArtifactActor
```

The recommended first mapping is one Notion page per logical Artifact. The page stores:

- Aster Artifact ID and version;
- Goal path and source Task path;
- Artifact kind, tags, and content hash;
- a link to the canonical content when the body is too large;
- rendered text or structured blocks for supported document kinds.

The Notion page ID is private integration state. It is stored in the Notion Actor's durable mapping and is never added to the Core Artifact schema.

Notion edits do not silently mutate Aster. An explicit `import` operation reads the page, validates its content, and publishes a new Artifact version with provenance pointing to the Notion page and edit timestamp.

## Knowledge Context flow

Notion or another external knowledge system may also be a Context source:

```text
Notion source Actor
    ↓
Context owner commit
    ↓
Context changes / query results
    ↓
System One, Goal gate, Task evidence, Memory capture
```

The source Actor must preserve the external page identity, source URL, retrieval time, and content hash. Evidence is frozen when admitted to a Goal or Task; later external edits do not rewrite an already accepted input.

Knowledge retrieval is on demand. A Goal should receive a bounded evidence result or a Task should retrieve the required page; the whole external workspace should not be loaded into a Goal prompt.

Artifact-sourced Context changes carry an origin marker or an equivalent Artifact/version/hash record. The sync Actor ignores a Context event that it already caused. This prevents an Artifact → Context → Artifact loop.

## Goal and Task integration

Goal tools:

- `artifact_list`;
- `artifact_get`;
- `artifact_publish`;
- `artifact_archive`;
- `artifact_restore`;
- `artifact_sync` for an explicit operator retry or target selection.

Task execution can publish an Artifact through the same Core catalog. The Task outcome retains only `ArtifactRef[]`, and Goal feedback includes those references when the result is delivered. The Task transcript remains in Pi.

The Agent prompt should distinguish:

- ordinary conversational output, which remains a Goal reply;
- durable work product, which requires `artifact_publish`;
- external publication, which requires an explicit `artifact_sync` or configured automatic target.

## Configuration

Configuration belongs at the application boundary and is decoded through Effect `Config` and `Schema`. A target entry contains:

```text
targetActorPath: absolute Actor path
operation: upsert | append
enabled: boolean
kinds?: ArtifactKind[]
tags?: string[]
on: publish | newVersion | manual
```

The host captures launch configuration once. Core receives typed settings and does not read environment variables or Notion credentials directly.

The target Actor owns external settings such as workspace IDs, database IDs, credentials, and provider-specific limits. Secrets use `Redacted` and never appear in Contexts, Artifacts, receipts, or logs.

## Public API

Add an Artifact RPC group:

```text
ListGoalArtifacts
GetArtifact
ReadArtifactContent
ArchiveArtifact
RestoreArtifact
SyncArtifact
```

Lists are cursor-paginated and support Goal, Task, kind, tag, status, and version filters. The public view includes provenance summaries and synchronization status, but excludes internal receipts, credentials, provider handles, and raw Actor paths unless explicitly needed for diagnostics.

Artifact notifications invalidate a Goal's artifact list and the relevant Artifact detail query. A synchronization status update does not create a new Artifact version.

## Error model

Expected failures use typed errors:

- `ArtifactNotFound`;
- `ArtifactConflict`;
- `ArtifactValidationError`;
- `ArtifactContentError`;
- `ArtifactPermissionError`;
- `ArtifactTargetUnavailable`;
- `ArtifactSyncRejected`;
- `ArtifactSyncUnknown`.

Defects, interruption, and infrastructure failures remain distinct from ordinary rejection. Recovery never converts an unknown external result into a successful or failed Artifact without evidence.

## Security and trust boundaries

- Validate all Artifact envelopes and target commands with Core schemas.
- Treat imported Notion or external Context content as untrusted evidence.
- Do not allow Artifact content to expand actor permissions or target destinations.
- Authorize publication against the current Goal owner and Task provenance.
- Do not accept arbitrary target Actor paths from model-generated arguments; model tools use configured targets or an operator-approved target.
- Store only stable identifiers in durable records.
- Enforce content size, MIME, and staging-retention limits in the content-store adapter.
- Preserve source hashes so a target can prove which Artifact version it represents.

## Testing strategy

Core tests use real isolated Actors, injected fake `ArtifactContentStore`, and a fake target Actor that implements the Core command.

Required cases include:

- creation and immutable version append;
- exact retry and changed-payload conflict;
- expected-version conflict;
- commit-before-ack for Artifact and content records;
- lost target acknowledgement and reconciliation;
- target rejection and explicit retry;
- Artifact Actor restart with pending synchronization;
- multiple targets receiving the same version independently;
- archived Goal retaining Artifacts;
- Task completion returning Artifact references;
- target Actor path unavailable during startup and after retirement;
- loop prevention for Artifact-originated Context changes;
- corrupted content metadata and hash mismatch;
- content staging cleanup after interrupted publication.

No real Notion API, model provider, external executor, or paid service is used in Core validation. The Notion integration gets adapter-level tests with a fake SDK boundary and a small number of separately isolated contract tests.

## Implementation slices

### Slice 1: Core contracts and local content

Add schemas, typed errors, `ArtifactContentStore`, `ArtifactCatalog`, in-memory content storage, and unit tests for version and receipt invariants.

### Slice 2: Artifact Actors and runtime registration

Add `ArtifactRootActor`, `ArtifactActor`, `ArtifactSyncActor`, recovery, and ActorPath selection. Register the root before Task and Goal roots.

### Slice 3: Goal and Task tools

Add publish/list/get tools, Task result references, Goal feedback rendering, and public Artifact views.

### Slice 4: Local storage and API

Add the local file content adapter, streaming reads, RPCs, invalidation keys, and UI-independent HTTP validation.

### Slice 5: Notion target Actor

Implement the Core command in `packages/integrations/src/notion`, persist Artifact-to-page mappings, add exact external-operation recovery, and support Aster-to-Notion upsert.

### Slice 6: Knowledge Context

Add Notion source retrieval, Context commits, frozen evidence, Memory capture, import-as-new-version, and loop protection.

## Acceptance criteria

The design is complete when:

1. Core can publish and read an Artifact without any integration package.
2. A fake target Actor can implement the Core sync command without Core importing that Actor.
3. A synchronization acknowledgement can be lost and recovered without duplicating the target projection.
4. A Task can publish a version and a Goal can list it by provenance.
5. A target Context can consume an Artifact version and expose it as bounded knowledge evidence.
6. A target edit can be imported as a new Artifact version with preserved provenance.
7. The Notion implementation can be removed without changing Artifact schemas or Goal/Task contracts.
