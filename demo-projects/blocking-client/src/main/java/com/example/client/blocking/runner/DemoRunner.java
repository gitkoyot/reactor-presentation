package com.example.client.blocking.runner;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class DemoRunner implements CommandLineRunner {

    private static final String BLOCKING_SERVER = "http://localhost:8081";
    private static final String REACTIVE_SERVER = "http://localhost:8082";
    // Default Tomcat = 200 threads. Difference shows when requests > 200.
    private static final int[] SCALING_STEPS = {50, 200, 500, 1000, 2000, 5000, 10000};

    private final RestTemplate restTemplate = new RestTemplate();

    @Override
    public void run(String... args) throws Exception {
        log.info("");
        log.info("=".repeat(78));
        log.info("  BLOCKING CLIENT — Benchmark Suite");
        log.info("  Blocking server: 200 Tomcat threads (default)  |  Reactive server: 4 event-loop threads");
        log.info("  Each request simulates 500ms I/O delay");
        log.info("=".repeat(78));
        log.info("");

        // ── WARM-UP ──
        log.info("Warming up...");
        singleRequest(BLOCKING_SERVER + "/api/products/1", "Blocking");
        singleRequest(REACTIVE_SERVER + "/api/products/1", "Reactive");
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

        List<long[]> rows = new ArrayList<>();

        for (int n : SCALING_STEPS) {
            long blockingWall = concurrentWallTime(BLOCKING_SERVER + "/api/products/", n);
            long reactiveWall = concurrentWallTime(REACTIVE_SERVER + "/api/products/", n);

            double speedup = reactiveWall > 0 ? (double) blockingWall / reactiveWall : 0;
            rows.add(new long[]{n, blockingWall, reactiveWall});

            log.info(String.format("║ %,14d │ %13d ms │ %13d ms │ %14.1fx    ║",
                    n, blockingWall, reactiveWall, speedup));
        }

        log.info("╠" + "═".repeat(76) + "╣");

        long[] first = rows.get(0), last = rows.get(rows.size() - 1);
        log.info("║" + center(
                String.format("Blocking: %dms → %dms (%.0fx slower as load grows)",
                        first[1], last[1], (double) last[1] / Math.max(1, first[1])), 76) + "║");
        log.info("║" + center(
                String.format("Reactive: %dms → %dms (stays flat — threads never wait!)",
                        first[2], last[2]), 76) + "║");
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
        long sseBlocking = concurrentSseTest(BLOCKING_SERVER + "/api/products/stream", "Blocking Server", 300);
        long sseReactive = concurrentSseTest(REACTIVE_SERVER + "/api/products/stream", "Reactive Server", 300);
        if (sseBlocking > 0 && sseReactive > 0) {
            log.info(String.format("  → Reactive was %.1fx faster for concurrent streaming!",
                    (double) sseBlocking / sseReactive));
        }
        log.info("");
    }

    // ─────────────────────────────────────────────────────────────
    private void singleRequest(String url, String name) {
        try {
            long start = System.nanoTime();
            restTemplate.getForObject(url, String.class);
            long ms = (System.nanoTime() - start) / 1_000_000;
            log.info(String.format("  %-12s %d ms", name + ":", ms));
        } catch (Exception e) {
            log.error(String.format("  %-12s ERROR", name + ":"), e);
        }
    }

    // ─────────────────────────────────────────────────────────────
    private long concurrentWallTime(String baseUrl, int count) {
        // Client pool must match request count so CLIENT isn't the bottleneck
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(count, 10000));
        CountDownLatch latch = new CountDownLatch(count);

        long wallStart = System.nanoTime();

        for (int i = 1; i <= count; i++) {
            final int id = (i % 50) + 1;
            executor.submit(() -> {
                try {
                    restTemplate.getForObject(baseUrl + id, String.class);
                } catch (Exception ignored) {
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executor.shutdown();

        return (System.nanoTime() - wallStart) / 1_000_000;
    }

    // ─────────────────────────────────────────────────────────────
    // SSE STREAMING TEST
    // ─────────────────────────────────────────────────────────────
    private long concurrentSseTest(String url, String serverName, int streamCount) {
        ExecutorService executor = Executors.newFixedThreadPool(streamCount);
        CountDownLatch latch = new CountDownLatch(streamCount);
        AtomicLong totalEvents = new AtomicLong(0);

        long wallStart = System.nanoTime();

        for (int i = 0; i < streamCount; i++) {
            executor.submit(() -> {
                try {
                    HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                    conn.setRequestProperty("Accept", "text/event-stream");
                    conn.setConnectTimeout(10_000);
                    conn.setReadTimeout(60_000);

                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(conn.getInputStream()))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            if (line.startsWith("data:")) {
                                totalEvents.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    // stream ended or connection error
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executor.shutdown();

        long wallTime = (System.nanoTime() - wallStart) / 1_000_000;
        log.info(String.format("  %-20s %d streams × 20 events = %d total | wall: %d ms",
                serverName + ":", streamCount, totalEvents.get(), wallTime));
        return wallTime;
    }

    // ─────────────────────────────────────────────────────────────
    // ASCII BAR CHART
    // ─────────────────────────────────────────────────────────────
    private void printAsciiChart(List<long[]> rows) {
        log.info("  Wall time (ms) by concurrent requests:");
        log.info("");

        long maxVal = rows.stream()
                .mapToLong(r -> Math.max(r[1], r[2]))
                .max().orElse(1);
        int barWidth = 50;

        for (long[] row : rows) {
            int bLen = (int) (row[1] * barWidth / maxVal);
            int rLen = (int) (row[2] * barWidth / maxVal);

            log.info(String.format("  %4d req  BLK │%-" + barWidth + "s│ %d ms",
                    row[0], "█".repeat(Math.max(1, bLen)), row[1]));
            log.info(String.format("           RCT │%-" + barWidth + "s│ %d ms",
                    "▓".repeat(Math.max(1, rLen)), row[2]));
            log.info("");
        }
    }

    // ─────────────────────────────────────────────────────────────
    private String center(String t, int w) {
        int p = (w - t.length()) / 2;
        return " ".repeat(Math.max(0, p)) + t + " ".repeat(Math.max(0, w - t.length() - p));
    }
}
