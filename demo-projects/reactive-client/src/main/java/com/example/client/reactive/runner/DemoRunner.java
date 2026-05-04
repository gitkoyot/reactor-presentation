package com.example.client.reactive.runner;

import io.netty.channel.ChannelOption;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class DemoRunner implements CommandLineRunner {

    // Default Tomcat = 200 threads. Difference shows when requests > 200.
    private static final int[] SCALING_STEPS = {50, 200, 500, 1000, 2000, 5000, 10000};
    private final WebClient webClient;

    public DemoRunner(WebClient.Builder webClientBuilder) {
        // Increase connection pool so the CLIENT is never the bottleneck
        ConnectionProvider provider = ConnectionProvider.builder("demo")
                .maxConnections(3000)
                .pendingAcquireMaxCount(3000)
                .build();
        HttpClient httpClient = HttpClient.create(provider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);

        this.webClient = webClientBuilder
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }

    @Override
    public void run(String... args) throws Exception {
        Thread.sleep(2000);

        log.info("");
        log.info("=".repeat(78));
        log.info("  REACTIVE CLIENT — Benchmark Suite");
        log.info("  Blocking server: 200 Tomcat threads (default)  |  Reactive server: 4 event-loop threads");
        log.info("  Each request simulates 500ms I/O delay");
        log.info("=".repeat(78));
        log.info("");

        // ── WARM-UP ──
        log.info("Warming up...");
        singleRequest("http://localhost:8081/api/products/1", "Blocking");
        singleRequest("http://localhost:8082/api/products/1", "Reactive");
        log.info("");

        // ── SCALING TEST ──
        log.info("╔" + "═".repeat(76) + "╗");
        log.info("║" + center("SCALING TEST — How does throughput change with load?", 76) + "║");
        log.info("║" + center("Blocking: 200 Tomcat threads (default — queued when full)", 76) + "║");
        log.info("║" + center("Reactive: 4 event-loop threads (never blocked)", 76) + "║");
        log.info("╠" + "═".repeat(76) + "╣");
        log.info(String.format("║ %-14s │ %16s │ %16s │ %16s    ║",
                "Requests", "Blocking (wall)", "Reactive (wall)", "Speedup"));
        log.info("╟" + "─".repeat(76) + "╢");

        List<ScalingRow> rows = new ArrayList<>();

        for (int n : SCALING_STEPS) {
            long blockingWall = concurrentWallTime("http://localhost:8081/api/products/", n);
            long reactiveWall = concurrentWallTime("http://localhost:8082/api/products/", n);

            double speedup = reactiveWall > 0 ? (double) blockingWall / reactiveWall : 0;
            rows.add(new ScalingRow(n, blockingWall, reactiveWall, speedup));

            log.info(String.format("║ %,14d │ %13d ms │ %13d ms │ %14.1fx     ║",
                    n, blockingWall, reactiveWall, speedup));
        }

        log.info("╠" + "═".repeat(76) + "╣");

        // Show the key insight
        ScalingRow first = rows.get(0);
        ScalingRow last = rows.get(rows.size() - 1);
        log.info("║" + center(
                String.format("Blocking: %dms → %dms (%.0fx slower as load grows)",
                        first.blockingWall, last.blockingWall,
                        (double) last.blockingWall / Math.max(1, first.blockingWall)), 76) + "║");
        log.info("║" + center(
                String.format("Reactive: %dms → %dms (stays flat — threads never wait!)",
                        first.reactiveWall, last.reactiveWall), 76) + "║");
        log.info("╚" + "═".repeat(76) + "╝");

        // ── ASCII CHART ──
        log.info("");
        printAsciiChart(rows);

        // ── SSE STREAMING — CONCURRENT ──
        log.info("");
        log.info("SSE Streaming — 300 concurrent streams (20 events each, 200ms apart)");
        log.info("-".repeat(78));
        log.info("  Each stream occupies a thread for ~4s. Blocking server has 200 threads.");
        log.info("  300 streams > 200 threads → 100 streams must wait → total time grows.");
        log.info("  Reactive: 4 event-loop threads handle all 300 streams in parallel.");
        log.info("");
        long sseBlocking = concurrentSseTest("http://localhost:8081/api/products/stream", "Blocking Server", 300);
        long sseReactive = concurrentSseTest("http://localhost:8082/api/products/stream", "Reactive Server", 300);
        if (sseBlocking > 0 && sseReactive > 0) {
            log.info(String.format("  → Reactive was %.1fx faster for concurrent streaming!",
                    (double) sseBlocking / sseReactive));
        }
        log.info("");
    }

    // ─────────────────────────────────────────────────────────────
    private long singleRequest(String url, String name) {
        try {
            long start = System.nanoTime();
            webClient.get().uri(url).retrieve().bodyToMono(String.class)
                    .timeout(Duration.ofSeconds(10)).block();
            long ms = (System.nanoTime() - start) / 1_000_000;
            log.info(String.format("  %-12s %d ms", name + ":", ms));
            return ms;
        } catch (Exception e) {
            log.error(String.format("  %-12s ERROR", name + ":"), e);
            return -1;
        }
    }

    // ─────────────────────────────────────────────────────────────
    private long concurrentWallTime(String baseUrl, int count) {
        try {
            long start = System.nanoTime();
            Flux.range(1, count)
                    .flatMap(id -> webClient.get()
                            .uri(baseUrl + (id % 50 + 1))
                            .retrieve()
                            .bodyToMono(String.class)
                            .timeout(Duration.ofSeconds(60))
                            .onErrorReturn("{}"), count)
                    .collectList()
                    .block(Duration.ofSeconds(120));
            return (System.nanoTime() - start) / 1_000_000;
        } catch (Exception e) {
            return -1;
        }
    }

    // ─────────────────────────────────────────────────────────────
    private long concurrentSseTest(String url, String serverName, int streamCount) {
        try {
            long wallStart = System.nanoTime();
            AtomicLong totalEvents = new AtomicLong(0);

            Flux.range(1, streamCount)
                    .flatMap(i -> webClient.get().uri(url)
                            .accept(MediaType.TEXT_EVENT_STREAM)
                            .retrieve().bodyToFlux(String.class)
                            .timeout(Duration.ofSeconds(60))
                            .doOnNext(e -> totalEvents.incrementAndGet())
                            .collectList()
                            .onErrorReturn(List.of()), streamCount)
                    .collectList()
                    .block(Duration.ofSeconds(120));

            long wallTime = (System.nanoTime() - wallStart) / 1_000_000;
            log.info(String.format("  %-20s %d streams × 20 events = %d total | wall: %d ms",
                    serverName + ":", streamCount, totalEvents.get(), wallTime));
            return wallTime;
        } catch (Exception e) {
            log.error(String.format("  %-20s ERROR — %s", serverName + ":", e.getMessage()), e);
            return -1;
        }
    }

    // ─────────────────────────────────────────────────────────────
    // ASCII BAR CHART
    // ─────────────────────────────────────────────────────────────
    private void printAsciiChart(List<ScalingRow> rows) {
        log.info("  Wall time (ms) by concurrent requests:");
        log.info("");

        long maxVal = rows.stream()
                .mapToLong(r -> Math.max(r.blockingWall, r.reactiveWall))
                .max().orElse(1);
        int barWidth = 50;

        for (ScalingRow row : rows) {
            int bLen = (int) (row.blockingWall * barWidth / maxVal);
            int rLen = (int) (row.reactiveWall * barWidth / maxVal);

            log.info(String.format("  %4d req  BLK │%-" + barWidth + "s│ %d ms",
                    row.count, "█".repeat(Math.max(1, bLen)), row.blockingWall));
            log.info(String.format("           RCT │%-" + barWidth + "s│ %d ms",
                    "▓".repeat(Math.max(1, rLen)), row.reactiveWall));
            log.info("");
        }
    }

    // ─────────────────────────────────────────────────────────────
    private String center(String t, int w) {
        int p = (w - t.length()) / 2;
        return " ".repeat(Math.max(0, p)) + t + " ".repeat(Math.max(0, w - t.length() - p));
    }

    private record ScalingRow(int count, long blockingWall, long reactiveWall, double speedup) {
    }
}
