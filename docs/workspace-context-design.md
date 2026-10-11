# Local Workspace Context Design

Status: proposed

## Purpose

Each Goal may have one persistent local working directory. The directory is the working surface for the Goal's pi-durable conversation and can also be exposed as a Context so internal Tasks can discover and read files through the normal Context query path.

The directory is the source of truth in this phase. The Context is a controlled read projection over that directory. It does not create Artifact versions, synchronize to Notion, or maintain a second copy of file contents.

```text
Goal
  ├── pi-durable ExecutionEnv (read/write for Goal conversation)
  └── WorkspaceContextActor (read-only Context queries)
          └── local workspace directory
```

The two consumers share a resolved workspace identity and root path, but they have different capabilities. The Agent may write through its workspace environment. Context queries expose bounded reads to Tasks and screening workflows. A Context query must never become an implicit file write.

## Identity and paths

The workspace has three distinct identities:

```text
workspaceId:  goal slug or stable workspace identifier
contextPath:  /workspaces/<workspace-id>
actorPath:    /user/workspaces/<workspace-id>
root:         resolved local filesystem directory
```

`contextPath` is a business Context path. `actorPath` is the local runtime endpoint. The runtime derives `actorPath` from the workspace definition; users and model tools do not provide Actor paths.

The default local root is derived below the resolved durable root:

```text
<config.durable.root>/workspaces/goals/<goal-slug>
```

The workspace remains one ordinary directory. The contract does not create or require `artifacts/`, `scratch/`, `tasks/`, or any other child directories. Users, Goals, and external tools choose their own layout.

## Configuration

Workspace configuration is module configuration captured by the host and decoded with Effect `Config` and `Schema`:

```yaml
goals:
  - slug: project
    workspace:
      mode: read-write
      context:
        enabled: true
        description: "Project files and working documents"
      shell: disabled
      maxFileBytes: 10485760
```

The `context.path` is derived as `/workspaces/project`. It is not independently configurable in the first version because deriving it from the stable Goal slug avoids aliases and duplicate owners. A later general workspace registry may support non-Goal workspaces with explicit IDs.

`mode` describes the Goal execution environment:

- `read-only`: Goal file tools can inspect files but cannot mutate them;
- `read-write`: Goal file tools can read, write, and edit within the root.

`context.enabled` controls whether the workspace appears in the configured Context directory and can be queried by Tasks. It does not enable file writes.

`shell` is independent from file access. `disabled` is the initial default. Enabling shell execution requires a separate execution policy and a stronger sandbox boundary; setting `cwd` is not sufficient isolation.

## WorkspaceContextActor

`WorkspaceContextActor` is a concrete local Context owner. It is implemented outside the domain-neutral Actor package and consumes the Core Context contracts plus an injected Effect `FileSystem` and `Path` capability. The Agent harness receives only a host-owned workspace environment factory; it does not construct a concrete local filesystem adapter.

The Actor owns:

- the stable `contextPath` and workspace description;
- root validation and file access policy;
- bounded read operations;
- query result pagination and temporary result identities;
- optional Git metadata reads;
- Context publication when an explicit refresh is requested.

It does not own Goal business state, Artifact versions, or the pi-durable conversation. It does not write Goal snapshots.

Runtime registration is analogous to other configured source Actors:

```text
/user/workspaces
  /project       WorkspaceContextActor
```

The root registers configured workspace children, watches their lifecycle, and supplies the resolved root path. It does not expose filesystem handles to the rest of the runtime.

## Context projection

The public Context projection contains metadata and a bounded textual summary:

```text
ContextText
  path: /workspaces/project
  description: Project files and working documents
  revision: committed workspace metadata revision
  text: rendered directory summary or explicit refresh summary
```

The projection does not embed every file or binary content. File contents are retrieved through scoped Context queries and returned as bounded query pages.

The initial Context commands are read-only:

```text
list_files(path?, recursive?, limit?)
read_file(path, offset?, limit?)
search_text(query, path?, limit?)
file_info(path)
git_status()
git_diff(path?)
```

These commands use the existing `ContextQueries` contract. Each command has a typed input, success schema, and error schema. The command adapter returns relative paths and source metadata, never absolute host paths.

Binary files are represented by metadata in `file_info`; the initial text query surface does not attempt to decode arbitrary binary formats. A later content-download boundary can add bounded binary reads without changing Context ownership.

## Query and evidence flow

```text
Configured Context directory
        ↓
Task list_contexts / describe_context
        ↓
Task query_context(/workspaces/project, ...)
        ↓
WorkspaceContextActor
        ↓
bounded result pages with path, hash, and retrieval time
        ↓
Goal feedback or Context evidence
```

Tasks discover the workspace through its Context description. A path hint does not grant permission or guarantee that a file exists; the Task must inspect the supported commands and handle typed query errors.

Evidence retains:

- workspace Context path;
- relative file path;
- file hash or metadata revision;
- retrieval timestamp;
- query command and normalized arguments.

If the file changes after evidence admission, the accepted evidence remains frozen. A later query obtains a new hash and a new result identity.

## Relationship to pi-durable

The Goal execution path creates a host-provided pi-durable `ExecutionEnv` with the workspace as `cwd`:

```text
Goal Harness
  cwd = workspace root
  tools = selected read/write/edit tools

Workspace Context
  root = same workspace root
  queries = list/read/search/info/status
```

The Goal can create and edit files across turns. Tasks can inspect those files through Context queries. The Goal Agent's write capability is not exposed through the Context query registry, and Context queries do not mutate files.

The Agent harness must preserve the workspace identity and policy when recovering a pending native submission. A recovered conversation cannot silently switch to a different directory.

Internal Tasks use the owning Goal workspace by default when they need local files. A separate Task directory is an optional execution policy and is not part of the default workspace layout. External executor workspaces remain owned by their executor adapters.

## File safety

The local adapter must enforce the workspace root for every operation:

- resolve relative paths against the assigned root;
- reject `..` traversal and absolute paths from model-facing inputs;
- reject symlink escapes after canonicalization;
- apply file-size, result-size, and recursion limits;
- exclude credentials and process metadata according to policy;
- avoid returning absolute paths in prompts, evidence, and public Context views;
- use scoped Effects for watchers and child processes;
- preserve cancellation and close readers on interruption.

Read-only Context queries use the same path safety policy as file tools. The policy is shared as an internal adapter capability, not as a generic mutable filesystem service.

## Refresh and change publication

The first version does not watch every filesystem write. A file watcher would create noisy Context events for editor saves and would require coalescing, durable admission, and loop protection.

The initial behavior is:

- queries read the current filesystem state;
- `refresh` is an explicit internal operation if the public Context summary needs updating;
- a refresh computes a bounded summary and commits it through the Context owner;
- no file write automatically creates a Context event;
- later Artifact promotion or a deliberate workspace watcher can add durable change events.

If a watcher is introduced later, it must publish stable file identities and hashes, retain unconfirmed changes until Context admission, and coalesce only changes that have not been frozen as evidence.

## Permissions and ownership

Workspace access follows the owning Goal:

- the Goal conversation may use the configured file mode;
- the Goal's internal Tasks may query the workspace through Context;
- external Tasks do not receive access merely because they reply to the Goal;
- the read-only relevance gate has no file write capability;
- a model cannot select an arbitrary workspace path or escape its Goal scope.

Cross-Goal sharing is not part of the first implementation. If a future Task needs another Goal's workspace, it receives an explicit evidence or file reference through a typed operation rather than discovering the directory by filesystem traversal.

## Failure and recovery

Expected filesystem failures become typed Context query errors:

- workspace unavailable;
- path not found;
- path escapes workspace;
- file too large;
- result truncated;
- unsupported binary content;
- permission denied.

A query never turns a missing or unreadable file into an empty successful result. A workspace Actor may become dormant when no query or refresh is active, but its durable Context metadata remains discoverable. A failed or retired Actor is reported as unavailable until runtime recovery restores it.

## Implementation slices

1. **Implemented first vertical slice:** local runtime derives per-owner workspaces below `<durable-root>/workspaces`, Goal conversations select `goals/<slug>`, and Goal Tasks select their owning Goal workspace by default.
2. **Implemented first adapter:** the host injects a Node `ExecutionEnv` into pi-durable with the Goal workspace as `cwd`; shell execution is disabled and the selected read/write/edit tools are installed only for conversations with a workspace identity. Strong path isolation remains a later pi-durable extension.
3. **Implemented recovery wiring:** the workspace extension and `cwd` are part of the durable agent configuration, so reopened conversations resolve the same workspace.
4. Add `WorkspaceContextActor` and its typed read-only Context queries.
5. Register workspaces in the configured Context directory and expose them to Task discovery.
6. Add tests for path safety, query pagination, file changes between reads, Goal restart, cancellation, and unavailable workspace recovery.
7. Evaluate explicit refresh and later file watching only after the read/write workspace path is stable.

## Future Artifact promotion

When Artifact management is implemented, it can consume this Context without changing the workspace contract:

```text
workspace file
    ↓ explicit artifact_publish(relativePath)
Artifact version with file hash and provenance
    ↓ optional Core ArtifactSyncCommand
target Actor or external Context
```

The workspace remains the local working surface. Artifact promotion adds durable identity and version history at an explicit operation boundary; it does not require a fixed directory layout.
