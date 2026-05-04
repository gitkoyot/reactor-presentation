package com.example.blocking.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/timing")
public class TimingController {

    @GetMapping("/health")
    public Map<String, Object> health() {
        long startNano = System.nanoTime();
        String threadName = Thread.currentThread().getName();

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "UP");
        response.put("timestamp", Instant.now().toString());

        long elapsedMs = (System.nanoTime() - startNano) / 1_000_000;
        log.info("GET /api/timing/health - Thread: {}, Elapsed: {}ms", threadName, elapsedMs);

        return response;
    }

    @GetMapping("/info")
    public Map<String, Object> info() {
        long startNano = System.nanoTime();
        String threadName = Thread.currentThread().getName();

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long threadCount = threadMXBean.getThreadCount();
        long peakThreadCount = threadMXBean.getPeakThreadCount();

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("serverType", "blocking");

        Map<String, Object> threadPool = new LinkedHashMap<>();
        threadPool.put("activeThreads", threadCount);
        threadPool.put("peakThreads", peakThreadCount);
        threadPool.put("maxPoolSize", 200);
        response.put("threadPoolInfo", threadPool);

        long elapsedMs = (System.nanoTime() - startNano) / 1_000_000;
        log.info("GET /api/timing/info - Thread: {}, Elapsed: {}ms, Active Threads: {}, Peak Threads: {}",
                threadName, elapsedMs, threadCount, peakThreadCount);

        return response;
    }
}
