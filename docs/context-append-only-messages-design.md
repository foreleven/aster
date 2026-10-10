# Append-only Context Messages

Status: accepted during the design interview on 2026-10-10.

Context Messages are immutable observations. A Context handle exposes append and remove operations; it does not expose update or upsert. Metadata preserves the order in which messages were appended. Message sorting is a domain concern outside the Context storage API.

The handle supplies keyOf(message) to derive the family member identity. Appending an existing key with identical content is an idempotent no-op. Appending an existing key with different content is a typed duplicate conflict. Duplicate keys within one append batch are rejected. Removing keys is idempotent when a key is already absent.

Summary completion uses one atomic commit that updates State and removes the selected observation keys:

    context.commit(current => ({
      state: nextState,
      messages: { remove: selected.map(keyOf) }
    }))

The old removeUnchanged operation compared a frozen message body before deleting it, which was necessary while the Context API permitted updates. Removing updates eliminates the in-place edit race. A summary domain may sort a detached append-ordered list for chronological batching, but that ordering is not persisted by Context.

Removal retires the message document and removes its key from the order index in the same transaction. Context retains no application tombstone, fingerprint, receipt, or historical identity record. Duplicate checks apply only to currently retained messages. Reusing a removed key creates a new family member and appends it at the end, including when its content matches the removed message.

Message deletion is idempotent while the key remains absent, but installing a rolling summary is a separate state transition. The domain owns summary generation checks and serializes summary application; it must not remove and reuse an in-flight batch's keys before applying that batch. Generic Context storage does not track summary jobs or enforce historical identity rules. Integration-specific ingress replay deduplication remains a separate domain concern.

For IM, a changed external payload under an existing observation key is rejected. If an integration needs to retain both versions, it must derive a new observation key before calling append.
