# Upstream source references

`effect/` contains the complete, unmodified Effect source at the release recorded in
[`effect-source.json`](effect-source.json). Its upstream MIT license is retained in
`effect/LICENSE`. It is reference material, excluded from the pnpm workspace,
application builds, linting, and formatting. Application imports use installed
packages, never this directory.

## Updating Effect

Update the workspace's Effect dependencies and this snapshot together. Verify the
release tag against the official npm package version and record the tag's exact
commit. Before replacing the snapshot, ensure `git status --short -- repos/effect`
shows no local changes.

Fetch the release tag from `https://github.com/Effect-TS/effect.git` without changing
the application branch. Export that commit with `git archive` to a temporary
directory, then synchronize it into `repos/effect/`, including removal of files
deleted upstream. Preserve the entire tree and license without local edits. Update
`effect-source.json`, maintained version references, and the lockfile. Do not install
dependencies or run builds inside the reference directory.

The original import used a squashed Git subtree. Subsequent release snapshots are
recorded as ordinary source changes with an exact upstream commit in the manifest;
the original subtree trailer is not the version authority. Review snapshot changes
separately from application changes, then run the workspace build, tests, browser
tests, and Effect diagnostics against the installed release.
