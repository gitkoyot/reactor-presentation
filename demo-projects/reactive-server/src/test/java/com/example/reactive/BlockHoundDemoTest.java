package com.example.reactive;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;

/**
 * BlockHound demo — detects blocking calls on non-blocking threads.
 * <p>
 * USAGE DURING PRESENTATION:
 * 1. Uncomment @Disabled on the test you want to show
 * 2. Run from IntelliJ — the test will FAIL with a clear error:
 * "Blocking call! java.lang.Thread.sleep" on a reactor thread
 * 3. Show how to fix it: offload to Schedulers.boundedElastic()
 * <p>
 * All tests are @Disabled by default so they don't break the build.
 */
@Slf4j
class BlockHoundDemoTest {

    static {
        try {
            BlockHound.install();
        } catch (IllegalStateException e) {
            log.warn("BlockHound initialization failed. Make sure to add JVM flag: -XX:+AllowRedefinitionToAddDeleteMethods", e);
        }
    }

    // ─────────────────────────────────────────────────────────────
    // TEST 1: BlockHound catches Thread.sleep() on a reactor thread
    // ─────────────────────────────────────────────────────────────
    @Test
    @Disabled("Enable during presentation to show BlockHound catching a blocking call")
    @DisplayName("BlockHound detects Thread.sleep() on non-blocking thread")
    void blockHound_catches_threadSleep() {
        // This pipeline runs Thread.sleep() on a Schedulers.parallel() thread.
        // BlockHound will throw: "Blocking call! java.lang.Thread.sleep"
        Mono<String> mono = Mono.delay(Duration.ofMillis(1))
                .map(tick -> {
                    try {
                        Thread.sleep(100); // ← BLOCKING CALL on reactor thread!
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "done";
                });

        StepVerifier.create(mono)
                .expectNext("done")
                .verifyComplete();
    }

    // ─────────────────────────────────────────────────────────────
    // TEST 2: The fix — offload blocking work to boundedElastic()
    // ─────────────────────────────────────────────────────────────
    @Test
    @Disabled("Enable during presentation to show the fix for blocking calls")
    @DisplayName("Fix: subscribeOn(Schedulers.boundedElastic()) offloads blocking work")
    void blockHound_passes_with_boundedElastic() {
        // Same blocking call, but wrapped in Mono.fromCallable + subscribeOn(boundedElastic).
        // BlockHound allows it because boundedElastic threads ARE allowed to block.
        Mono<String> mono = Mono.fromCallable(() -> {
                    Thread.sleep(100); // ← blocking, but on a boundedElastic thread — OK!
                    return "done";
                })
                .subscribeOn(Schedulers.boundedElastic());

        StepVerifier.create(mono)
                .expectNext("done")
                .verifyComplete();
    }

    // ─────────────────────────────────────────────────────────────
    // TEST 3: Simulates a sneaky JDBC-like blocking call
    // ─────────────────────────────────────────────────────────────
    @Test
    @Disabled("Enable during presentation to show a realistic blocking I/O scenario")
    @DisplayName("BlockHound detects simulated JDBC blocking call in reactive pipeline")
    void blockHound_catches_jdbc_simulation() {
        // Simulates what happens when you accidentally call a blocking repository
        // method inside a reactive pipeline without offloading it.
        Mono<String> mono = Mono.delay(Duration.ofMillis(1))
                .flatMap(tick -> {
                    // Pretend this is a JDBC call
                    String result = simulateJdbcQuery();
                    return Mono.just(result);
                });

        StepVerifier.create(mono)
                .expectNext("db-result")
                .verifyComplete();
    }

    @Test
    @Disabled("Enable during presentation to show how to properly wrap blocking JDBC")
    @DisplayName("Fix: wrap JDBC in Mono.fromCallable + boundedElastic")
    void blockHound_passes_with_wrapped_jdbc() {
        Mono<String> mono = Mono.defer(() ->
                Mono.fromCallable(this::simulateJdbcQuery)
                        .subscribeOn(Schedulers.boundedElastic())
        );

        StepVerifier.create(mono)
                .expectNext("db-result")
                .verifyComplete();
    }

    // ─────────────────────────────────────────────────────────────
    private String simulateJdbcQuery() {
        try {
            Thread.sleep(50); // simulates blocking JDBC call
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return "db-result";
    }

}
