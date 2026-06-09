package com.example.client.blocking.runner;

import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
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

    private final RestTemplate restTemplate = buildPooledRestTemplate();

    /**
     * RestTemplate over a pooled Apache HttpClient. The default factory
     * (HttpURLConnection) caches only ~5 keep-alive connections per host — at
     * 10,000 concurrent requests nearly every call opens a fresh TCP connection,
     * the SYN storm overflows the server accept backlog (Windows replies RST) and
     * retries amplify it. Pooling reuses connections across requests and scaling
     * steps, same as the WebClient ConnectionProvider in reactive-client.
     * Still 100% blocking — the calling thread waits for the response.
     */
    private static RestTemplate buildPooledRestTemplate() {
        var connectionManager = PoolingHttpClientConnectionManagerBuilder.create()
                .setMaxConnTotal(12000)
                .setMaxConnPerRoute(12000)
                .build();
        var httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .build();
        return new RestTemplate(new HttpComponentsClientHttpRequestFactory(httpClient));
    }

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
        log.info(String.format("║ %-9s │ %13s │ %7s │ %13s │ %7s │ %10s ║",
                "Requests", "Blocking", "B fail", "Reactive", "R fail", "Speedup"));
        log.info("╟" + "─".repeat(76) + "╢");

        List<long[]> rows = new ArrayList<>();

        for (int n : SCALING_STEPS) {
            long[] blocking = concurrentWallTime(BLOCKING_SERVER + "/api/products/", n);
            long[] reactive = concurrentWallTime(REACTIVE_SERVER + "/api/products/", n);

            double speedup = reactive[0] > 0 ? (double) blocking[0] / reactive[0] : 0;
            rows.add(new long[]{n, blocking[0], blocking[1], reactive[0], reactive[1]});

            log.info(String.format("║ %,9d │ %10d ms │ %7d │ %10d ms │ %7d │ %9.1fx ║",
                    n, blocking[0], blocking[1], reactive[0], reactive[1], speedup));
        }

        log.info("╠" + "═".repeat(76) + "╣");

        long[] first = rows.get(0), last = rows.get(rows.size() - 1);
        log.info("║" + center(
                String.format("Blocking: %dms → %dms (%.0fx slower as load grows)",
                        first[1], last[1], (double) last[1] / Math.max(1, first[1])), 76) + "║");
        log.info("║" + center(
                String.format("Reactive: %dms → %dms (stays flat — threads never wait!)",
                        first[3], last[3]), 76) + "║");
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
    /** @return {wallTimeMs, failedRequests} */
    private long[] concurrentWallTime(String baseUrl, int count) {
        // Client pool must match request count so CLIENT isn't the bottleneck
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(count, 10000));
        CountDownLatch latch = new CountDownLatch(count);

        AtomicLong errors = new AtomicLong(0);
        AtomicLong retries = new AtomicLong(0);
        long wallStart = System.nanoTime();

        for (int i = 1; i <= count; i++) {
            final int id = (i % 50) + 1;
            executor.submit(() -> {
                try {
                    getWithRetry(baseUrl + id, retries);
                } catch (Exception e) {
                    errors.incrementAndGet();
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
        if (retries.get() > 0) {
            log.info("      ↳ {} refused connects recovered by retry  [{}]", retries.get(), baseUrl);
        }
        return new long[]{wallTime, errors.get()};
    }

    /**
     * GET with retry on "connection refused" only. Windows caps the listen backlog —
     * under a connection storm some SYNs get RST; a short backoff recovers them.
     */
    private void getWithRetry(String url, AtomicLong retries) throws Exception {
        int attempt = 0;
        while (true) {
            try {
                restTemplate.getForObject(url, String.class);
                return;
            } catch (Exception e) {
                if (!isConnectionRefused(e) || ++attempt >= 5) {
                    throw e;
                }
                retries.incrementAndGet();
                Thread.sleep(200L * attempt); // 200, 400, 600, 800 ms
            }
        }
    }

    private static boolean isConnectionRefused(Throwable e) {
        Throwable t = e;
        while (t != null) {
            if (t instanceof ConnectException) {
                return true;
            }
            t = (t.getCause() != t) ? t.getCause() : null;
        }
        return false;
    }

    // ─────────────────────────────────────────────────────────────
    // SSE STREAMING TEST
    // ─────────────────────────────────────────────────────────────
    private long concurrentSseTest(String url, String serverName, int streamCount) {
        ExecutorService executor = Executors.newFixedThreadPool(streamCount);
        CountDownLatch latch = new CountDownLatch(streamCount);
        AtomicLong totalEvents = new AtomicLong(0);
        AtomicLong failedStreams = new AtomicLong(0);

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
                    failedStreams.incrementAndGet(); // connection error / premature end
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
        if (failedStreams.get() > 0) {
            log.warn("  ⚠ {}/{} streams FAILED against {}", failedStreams.get(), streamCount, url);
        }
        return wallTime;
    }

    // ─────────────────────────────────────────────────────────────
    // ASCII BAR CHART
    // ─────────────────────────────────────────────────────────────
    private void printAsciiChart(List<long[]> rows) {
        log.info("  Wall time (ms) by concurrent requests:");
        log.info("");

        long maxVal = rows.stream()
                .mapToLong(r -> Math.max(r[1], r[3]))
                .max().orElse(1);
        int barWidth = 50;

        for (long[] row : rows) {
            int bLen = (int) (row[1] * barWidth / maxVal);
            int rLen = (int) (row[3] * barWidth / maxVal);

            log.info(String.format("  %4d req  BLK │%-" + barWidth + "s│ %d ms",
                    row[0], "█".repeat(Math.max(1, bLen)), row[1]));
            log.info(String.format("           RCT │%-" + barWidth + "s│ %d ms",
                    "▓".repeat(Math.max(1, rLen)), row[3]));
            log.info("");
        }
    }

    // ─────────────────────────────────────────────────────────────
    private String center(String t, int w) {
        int p = (w - t.length()) / 2;
        return " ".repeat(Math.max(0, p)) + t + " ".repeat(Math.max(0, w - t.length() - p));
    }
}
