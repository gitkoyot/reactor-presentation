# Spring WebFlux — Reactive vs Blocking

Presentation and live demo projects comparing Spring MVC (blocking) with Spring WebFlux (reactive) under increasing load.

## Repository Structure

```
├── WebFlux-Reactive-vs-Blocking.pptx   # Slide deck (33 slides)
└── demo-projects/                       # Gradle multi-module project (Java 21)
    ├── blocking-server   :8081          # Spring MVC + Tomcat (200 threads)
    ├── reactive-server   :8082          # Spring WebFlux + Netty (4 event-loop threads)
    ├── blocking-client   :8083          # RestTemplate benchmark client
    └── reactive-client   :8084          # WebClient benchmark client
```

## Quick Start

```bash
cd demo-projects

# 1. Start both servers (separate terminals)
./gradlew :blocking-server:bootRun
./gradlew :reactive-server:bootRun

# 2. Run either benchmark client
./gradlew :blocking-client:bootRun
./gradlew :reactive-client:bootRun
```

## What the Demo Shows

Each server endpoint simulates **500ms I/O delay** per request. Clients fire scaling batches of concurrent requests (50 → 200 → 500 → 1,000 → 2,000 → 5,000 → 10,000) and measure wall-clock time.

**Blocking server** — 200 Tomcat threads process 200 requests per 500ms round. Beyond 200 concurrent requests, threads queue up and total time grows linearly.

**Reactive server** — 4 Netty event-loop threads release during the delay, handling thousands of concurrent requests in well under a second.

Both clients also run a **300 concurrent SSE streams** test (20 events each, 200ms apart) to show how streaming amplifies the difference.

### Honest benchmarking

The scaling table reports **B fail / R fail** columns — requests that ultimately failed — plus a per-step line with the number of refused connects recovered by retry. A result only counts if all 10,000 requests actually received a response; failures are never silently swallowed.

Both clients use connection pooling so the client is never the bottleneck: reactive-client via a Reactor Netty `ConnectionProvider` (10,000 connections), blocking-client via Apache HttpClient 5 pooling under `RestTemplate` (the default `HttpURLConnection` keeps only ~5 keep-alive connections per host, which causes a SYN storm at high concurrency). Both retry *connection-refused* errors only, with a short backoff.

### High-concurrency tuning (the 10,000-request step)

- **blocking-server** — `server.tomcat.max-connections: 12000`, `accept-count: 10000` (defaults 8192/100 reject part of the burst at the socket level).
- **reactive-server** — `SO_BACKLOG 8192` and a dedicated acceptor loop (`reactor.netty.ioSelectCount=1`); without it the 4 busy worker loops also handle `accept()` and the queue overflows.
- **Windows only** — widen the ephemeral port range once (admin), otherwise ~16k default ports are exhausted by two 10k-connection pools:

  ```
  netsh int ipv4 set dynamicport tcp start=10000 num=55000
  ```

  Windows also actively refuses (RST) connections when the listen backlog is full — Linux drops the SYN and lets TCP retransmit, so on Linux these tweaks matter less.

## Tech Stack

- Java 21, Spring Boot 3.3.5, Gradle (Kotlin DSL)
- Spring MVC / Spring WebFlux
- Project Reactor (Mono, Flux)
- Apache HttpClient 5 (pooled `RestTemplate` in blocking-client)
- Lombok (`@Slf4j`), Logback
- reactor-test / StepVerifier, BlockHound, ReactorDebugAgent (reactive-server)

## Presentation Topics

Fundamentals (blocking vs non-blocking I/O, event loop model), Project Reactor (Mono/Flux, operators, marble diagrams, Reactive Streams spec, signals, backpressure, Hot vs Cold, schedulers, error handling), code examples, live demo, testing with StepVerifier and virtual time, debugging with checkpoint() and BlockHound, challenges, and when to use WebFlux vs MVC.
