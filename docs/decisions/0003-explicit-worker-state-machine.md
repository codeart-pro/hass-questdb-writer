# ADR 0003: Use an explicit worker state machine

Status: accepted for the independent worker implementation.

Date: 2026-08-29

## Context

The original QSS failure mode allowed a retry wrapper to exhaust its policy and
raise `tenacity.RetryError` out of the worker path. The thread terminated, while
Home Assistant could continue producing events without a live delivery owner.

The replacement must distinguish temporary transport failure, uncertain remote
commit, deterministic row rejection, authentication/configuration failure,
local spool pressure, lifecycle stop, and unexpected code failure. It must
continue persisting accepted events during a network backoff and make thread
death observable.

## Decision

Use one explicit, dependency-free state machine in `WriterService`:

- one daemon thread owns SQLite and HTTP resources;
- `start()` waits for resource initialization and exposes startup failure;
- `submit()` is bounded and non-blocking;
- events are acknowledged from ingress only after a durable spool transaction;
- batch delivery is triggered by row, byte, or latency bounds;
- retryable errors retain rows and schedule capped exponential backoff with
  bounded symmetric jitter;
- no retry helper is allowed to raise a terminal wrapper exception;
- new ingress is persisted while remote delivery is in retry wait;
- a multi-row HTTP 400 switches to single-row isolation;
- only an individually rejected HTTP 400 row enters dead-letter;
- authentication and other permanent configuration failures block delivery but
  do not delete pending rows;
- the outer thread boundary records every unexpected exception as `failed`;
- snapshots are immutable and never read the worker-owned SQLite connection;
- stop first rejects new submissions, then persists accepted ingress, optionally
  attempts one ready remote flush, closes resources, and joins by a caller
  deadline.

The service is one-shot. Reload creates a new service only after the old one has
stopped. The thread is daemonized as a final process-exit safeguard, but normal
operation always uses explicit stop and join.

## Delivery acknowledgement

Rows are deleted only after `IlpHttpTransport.send_batch()` returns an HTTP 2xx
response. QuestDB WAL rows may become SQL-visible shortly after that response;
SQL visibility polling is a test concern, not the delivery acknowledgement
mechanism.

If the server commits but the client loses the response, the retry is marked
delivery-uncertain and can create a duplicate. That flag is sticky in SQLite.

## Consequences

Positive:

- retry exhaustion cannot silently terminate the thread;
- every state transition is testable without sleeping inside a decorator;
- outage ingestion and remote replay are independent;
- deterministic bad rows cannot block all later rows indefinitely;
- lifecycle and health reporting use one coherent state model.

Negative:

- this project owns more state-machine code and tests;
- a permanently blocked destination eventually fills the bounded spool;
- stopping cannot interrupt a Python HTTP call already blocked in the socket;
  the transport timeout must therefore fit the lifecycle stop budget;
- batch HTTP 400 isolation adds repeated requests and attempt metadata;
- exact queue, retry, and shutdown defaults remain unselected.

## Verification completed

The native ARM64 Home Assistant 2026.7.2 test environment covers:

- threshold batch delivery and health counters;
- retryable failure followed by automatic recovery;
- persistence of new ingress while waiting for retry;
- sticky uncertain-delivery metadata;
- multi-row HTTP 400 isolation to one dead-letter row;
- authentication block with continued local spooling;
- invalid persisted payload handling;
- bounded ingress overflow and oversized-event rejection;
- shutdown with and without one remote flush;
- startup exception and startup timeout;
- bounded join while a transport call remains blocked;
- unexpected transport exception with the row still pending;
- real QuestDB delivery through the complete event/spool/ILP/HTTP path;
- real connection refusal followed by relay availability and reconnect.

## Required verification before production

- sustained QuestDB outage until configured spool pressure;
- Home Assistant reload while connected, retrying, blocked, and flushing;
- process termination during HTTP request and after uncertain commit;
- exact production event rate and serialized-size distribution;
- transport timeout versus Home Assistant shutdown budget;
- rate-limited logging and repair guidance for every blocked/failed state.
