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

**Blocking server** — 200 Tomcat threads process 200 requests per 500ms round. At 2,000+ requests, threads queue up and total time grows linearly.

**Reactive server** — 4 Netty event-loop threads release during the delay, handling thousands of concurrent requests in ~500ms flat.

Both clients also run a **300 concurrent SSE streams** test (20 events each, 200ms apart) to show how streaming amplifies the difference.

## Tech Stack

- Java 21, Spring Boot 3.3.5, Gradle (Kotlin DSL)
- Spring MVC / Spring WebFlux
- Project Reactor (Mono, Flux)
- Lombok (`@Slf4j`), Logback
- reactor-test / StepVerifier (reactive-server tests)

## Presentation Topics

Fundamentals (blocking vs non-blocking I/O, event loop model), Project Reactor (Mono/Flux, operators, marble diagrams, Reactive Streams spec, signals, backpressure, Hot vs Cold, schedulers, error handling), code examples, live demo, testing with StepVerifier and virtual time, debugging with checkpoint() and BlockHound, challenges, and when to use WebFlux vs MVC.
