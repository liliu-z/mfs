# MFS domain language

MFS accepts or observes source files, prepares searchable documents, and owns the lifecycle of processing and indexing work. This glossary defines shared terms. The current contract is in [docs/design.md](docs/design.md); remaining work is in [docs/backlog.md](docs/backlog.md).

## Identity, ownership, and text

| Term | Meaning |
| --- | --- |
| **Source** | Original input. An External Source is user-owned, observed through sync, and never copied/modified by MFS. An Internal Source is accepted, stored, and deleted by MFS. Distinguish source updates from updates to search results. |
| **Document** | Logical retrieval object identified by Namespace and Document ID. Content changes do not change its identity. |
| **Namespace** | A set of documents with stable identity, source ownership, and its own processing/retrieval configuration. Shared physical storage does not merge document identity. It is neither an independent database nor an authorization tenant. |
| **Namespace Incarnation** | Private, non-reusable identity for one creation of a Namespace. Recreating the same public name creates a different incarnation. |
| **Adapter Manifest** | Persistent compatibility requirements for processing, chunking, and embedding implementations. It is not a runtime object, model factory, or credential store. |
| **Source Policy** | One Namespace's ordered input rules, without global inheritance. Excluded input cannot reappear through another MFS retrieval method. |
| **Search Scope** | One explicitly selected Namespace, optionally narrowed by paths or document identities. Current-work waits separately select a document or namespace/path. |
| **Source Revision** | An accepted target version. It does not imply text/index readiness and is not a retry counter. Private input versions identify source observations independently of configuration generations. |
| **Document Target** | The latest desired processed version, or deletion intent, for a Document. Repeated notifications coalesce; it is not a historical event queue. |
| **Snapshot** | Identity and source-location data for a processing result. It is not a source backup, a Milvus snapshot, or an immutable External file; historical version reads are not provided. |
| **Search Text** | Text used for retrieval, read directly from textual sources or produced by necessary extraction. Grep and ranked search can use different views. Artifacts do not automatically become Search Text. |
| **Search Text File** | File containing Search Text: an original, managed extraction, or borrowed application output. A reference implies neither ownership nor that indexing has caught up with external changes. |
| **SourceMap** | Maps half-open UTF-8 byte ranges in processed Search Text to source pages, lines, or time ranges. It describes processed text, not original binary offsets or chunking. |
| **Chunk** | A contiguous range selected by a Chunker, with document identity, ordinal, and source location. Ranges may overlap; equal text at different positions still represents distinct occurrences. |
| **Artifact** | An immutable attachment published with prepared text and opened through `open_artifact`. A read lease protects its lifetime. |
| **Computation Reuse** | Reuse of compatible processing/vector results to avoid recomputation. It does not merge Documents, discard repeated occurrences, or restore stale visibility. |

## Retrieval and completion

| Term | Meaning |
| --- | --- |
| **Grep** | Literal/regex text matching or structured source path/name matching. Direct External text is read from disk; binary sources require prepared text; metadata-only filtering needs no body. It is independent of ranked-index readiness. |
| **Indexed Search** | BM25, vector, or hybrid ranking within one Namespace. Its readiness is distinct from text being available to Grep. |
| **Accepted** | MFS has durably recorded the request and owns its remaining work. External acceptance stores references without guaranteeing later availability. Accepted does not mean Indexed or Ready. |
| **Published** | A file's complete index result is eligible in the active generation; BM25 and dense complete together where required. Configuration promotion can publish successful members together while retaining unsuccessful member status. Partial publication does not imply Namespace readiness. |
| **Index Build** | Work producing prepared text, Chunks, and index vectors from accepted input. Producing outputs alone does not make them searchable. |
| **Ready** | The declared text or indexing target has completed. Text readiness does not imply index readiness or reflect external changes not yet observed. |
| **Strong** | Retrieval that waits for relevant current targets to become Ready. Updates can occur after admission; this is not query snapshot isolation. Strong Grep waits for text, strong Indexed Search for the selected Namespace's index. |
| **Eventual** | Retrieval from currently eligible text/publications without waiting for completion. Results may be incomplete; invalidated sources are never entitled to remain visible. |
| **Mutation / Sync Report** | Acceptance/observation outcome of a call, not a permanent completion receipt. `wait(report)` follows current file/scope work, including subsequent accepted updates. |
| **Current Work Wait** | Checks current file/scope targets and relevant Namespace control work until completion or error. It does not look up historical success. An idempotency key deduplicates acceptance, not readiness. |

## Execution and recovery

| Term | Meaning |
| --- | --- |
| **Processing Attempt** | One actual invocation with its own work directory, cancellation signal, and attempt token. Checkpoints can let a later invocation resume from a durable boundary. The token prevents stale commits. |
| **Active Run** | A file's durable logical processing chain, identified by `active_run_id`, capturing input/configuration, stage/state, and resume data. It can outlive a stage invocation. Losing commit authority does not mean the invocation has exited. |
| **Configuration Generation** | One processing/index manifest with its private text and collection. Active serves queries; building catches up current membership; retiring waits for actual readers/writers before cleanup. A configuration change alone does not invalidate unchanged sources. |
| **Index Generation** | Retrieval data built from observed/prepared text under an index configuration. It does not freeze the external source bytes. |
| **Deletion Work** | Revokes search eligibility and removes projections/managed outputs. Responsibility survives disappearance from document listings. External originals are never deleted. |
| **Cleanup Debt** | Durable physical deletion responsibility keyed by Namespace incarnation, collection generation, DocumentId, and snapshot, independent of current target success/failure. Cancellation and coalescing cannot discard it. |
| **User Cancellation Gate** | Persistent user intent to stop a file, retained across ordinary source observations. Explicit retry/reprocess clears it; internal supersession, drop, quiescence, and close do not create it. |
| **Scope Lease** | Temporary restriction on execution and source reads for explicit incarnation/path scopes, acquired after existing users actually retire. Releasing it removes only its own restriction. Host disk operations and sync still require host serialization. |
| **Processing Pause** | Persistent Namespace admission gate for preparation and new indexing, distinct from index-only pause. It survives reopen, allows cleanup, and does not prove active executions have retired. |
| **Startup Gate** | Process-local gate installed with `start_paused` before background execution. The host recovers its persistent disk journal before `resume_background`; the gate is not the journal or a user cancellation. |
| **Blocking Reason** | Computed explanation of why work cannot currently run, such as binding, pause, resources, quiescence, or retirement. It complements task state without creating another readiness state machine. |
