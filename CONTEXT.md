# wiki-stream-lab

The domain language of a Kafka learning lab that ingests the Wikimedia Recent Changes firehose and projects it into read models. This glossary is the ubiquitous language for the *data*, not the Kafka mechanics (those live in `docs/adr/`).

## Language

**Recent Change**:
One event from the Wikimedia firehose representing a single wiki action — an edit, a new page, a log action, or a categorization.
_Avoid_: edit (that's only one kind of Recent Change), change, update.

**Raw Event**:
A Recent Change exactly as received from the SSE stream, before validation, written to `wikimedia.recentchange.raw`.
_Avoid_: message, record, payload.

**Validated Event**:
A Recent Change that passed schema validation and was transformed into the internal event shape, written to `wikimedia.recentchange.validated`.
_Avoid_: clean event, parsed event.

**Dead Letter**:
A Recent Change that failed validation, routed to `wikimedia.dead_letter` with a failure reason and its source partition/offset.
_Avoid_: error event, rejected record, bad message.

**Page Activity**:
The per-page read model: edit count, bot-edit count, last editor, and last-event time for a single wiki page. The thing the projection builds.
_Avoid_: page stats, page record, page state.

**Bot Edit**:
A Recent Change where `bot` is true — an action performed by an automated account rather than a human.
_Avoid_: automated edit, machine edit.

**Wiki**:
A single Wikimedia project instance, identified by its database name (`enwiki`, `commonswiki`, `wikidatawiki`). The unit of the partition key's first half.
_Avoid_: site, project (broader), wikipedia (only one of many wikis).
