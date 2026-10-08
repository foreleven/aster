# Upstream source references

`effect/` contains the complete, unmodified Effect source at the release recorded in
[`effect-source.json`](effect-source.json). Its upstream MIT license is retained in
`effect/LICENSE`. The entire directory is local reference material, fully ignored
by Git and excluded from the pnpm workspace, application builds, linting, and
formatting. Only this guide and the source manifest are tracked. Application imports
use installed packages, never this directory.

## Restoring Effect

Fresh checkouts do not contain `repos/effect/`. Run the following from the workspace
root to restore the exact snapshot recorded in the manifest. The destination must
not already exist; move any existing directory to a backup before restoring it.

```sh
effect_repository=$(node -p 'require("./repos/effect-source.json").repository')
effect_tag=$(node -p 'require("./repos/effect-source.json").tag')
effect_commit=$(node -p 'require("./repos/effect-source.json").commit')
effect_temp=$(mktemp -d)
git clone --depth 1 --single-branch --branch "$effect_tag" "$effect_repository" "$effect_temp/upstream" &&
  test "$(git -C "$effect_temp/upstream" rev-parse HEAD)" = "$effect_commit" &&
  git -C "$effect_temp/upstream" archive --format=tar --output="$effect_temp/effect.tar" HEAD &&
  mkdir repos/effect &&
  tar -xf "$effect_temp/effect.tar" -C repos/effect
```

The temporary clone and archive can be removed after verifying the restored tree.
Do not install dependencies or run builds inside the reference directory.

## Updating Effect

Resolve the latest stable release with `npm view effect dist-tags.latest`, verify
its `effect@<version>` tag in `https://github.com/Effect-TS/effect.git`, and record
the tag's exact commit and package version in `effect-source.json`. Back up the
existing directory before replacing it: Git cannot report or recover local changes
inside this ignored directory. Restore the new snapshot using the steps above,
preserving the entire upstream tree and license without local edits.

Reference updates do not automatically upgrade application dependencies. When the
snapshot differs from the installed Effect version, verify relevant APIs against
the installed source or an exact matching snapshot before implementing changes.
Dependency upgrades must update package manifests, the lockfile, and maintained
version references together, then run the workspace validation required by
`AGENTS.md`.

The source was previously tracked as a subtree and release snapshots. It is now
local-only; do not re-add it with `git add -f` or `git subtree`. Verify that
`git ls-files -- repos/effect` is empty and `git check-ignore repos/effect/LLMS.md`
matches the root ignore rule.
