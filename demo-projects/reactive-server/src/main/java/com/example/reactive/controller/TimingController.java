package com.example.reactive.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/timing")
public class TimingController {

    /**
     * GET /api/timing/health
     * Returns health status with current timestamp
     */
    @GetMapping("/health")
    public Mono<Map<String, Object>> health() {
        long startNano = System.nanoTime();
        String threadName = Thread.currentThread().getName();

        return Mono.defer(() -> {
            Map<String, Object> response = new HashMap<>();
            response.put("status", "UP");
            response.put("timestamp", Instant.now().toString());
            response.put("serverType", "reactive");

            return Mono.just(response)
                    .doOnSuccess(r -> {
                        long elapsedMs = (System.nanoTime() - startNano) / 1_000_000;
                        log.debug("GET /api/timing/health - thread: {}, elapsed: {}ms",
                                threadName, elapsedMs);
                    });
        });
    }

    /**
     * GET /api/timing/info
     * Returns server information including server type and thread details
     */
    @GetMapping("/info")
    public Mono<Map<String, Object>> info() {
        long startNano = System.nanoTime();
        String threadName = Thread.currentThread().getName();

        return Mono.defer(() -> {
            Map<String, Object> threadInfo = new HashMap<>();
            threadInfo.put("name", threadName);
            threadInfo.put("id", Thread.currentThread().getId());
            threadInfo.put("state", Thread.currentThread().getState().toString());

            Map<String, Object> response = new HashMap<>();
            response.put("serverType", "reactive");
            response.put("framework", "Spring WebFlux");
            response.put("timestamp", Instant.now().toString());
            response.put("thread", threadInfo);

            return Mono.just(response)
                    .doOnSuccess(r -> {
                        long elapsedMs = (System.nanoTime() - startNano) / 1_000_000;
                        log.debug("GET /api/timing/info - thread: {}, elapsed: {}ms",
                                threadName, elapsedMs);
                    });
        });
    }
}
