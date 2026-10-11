# Core owns the application business API

Status: accepted; implementation pending the core design interview.

Application RPC handlers currently select internal Actors, construct domain commands and interpret private snapshots, which duplicates business behavior across transport and internal query paths. Core will provide one application business API owning those operations and public projections; the API package will adapt RPC transport and schemas. Application callers will no longer receive Actor selectors or internal snapshots, while integrations and infrastructure retain explicit capability entry points and Actor-local tools retain their serialized owner services.
