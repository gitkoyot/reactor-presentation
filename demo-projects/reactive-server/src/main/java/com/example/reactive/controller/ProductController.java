package com.example.reactive.controller;

import com.example.reactive.model.Product;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;

@Slf4j
@RestController
@RequestMapping("/api/products")
public class ProductController {

    /**
     * GET /api/products/{id}
     * Simulates database lookup with 500ms delay
     * Logs thread name and elapsed time via doOnSuccess
     */
    @GetMapping("/{id}")
    public Mono<Product> getProductById(@PathVariable Long id) {
        return Mono.defer(() -> {
            long startNano = System.nanoTime();
            String threadName = Thread.currentThread().getName();

            return Mono.just(new Product(id, "Product " + id, BigDecimal.valueOf(10.0 + id),
                    "Description for product " + id))
                    .delayElement(Duration.ofMillis(500))
                    .doOnSuccess(product -> {
                        long elapsedMs = (System.nanoTime() - startNano) / 1_000_000;
                        log.debug("GET /api/products/{} completed - thread: {}, elapsed: {}ms",
                                id, threadName, elapsedMs);
                    });
        });
    }

    /**
     * GET /api/products
     * Returns Flux of 10 products with 100ms delay between each
     * Logs timing via doOnComplete
     */
    @GetMapping
    public Flux<Product> getAllProducts() {
        long startNano = System.nanoTime();
        String threadName = Thread.currentThread().getName();

        return Flux.range(1, 10)
                .map(id -> new Product((long) id, "Product " + id, BigDecimal.valueOf(10.0 + id),
                        "Description for product " + id))
                .delayElements(Duration.ofMillis(100))
                .doOnComplete(() -> {
                    long elapsedMs = (System.nanoTime() - startNano) / 1_000_000;
                    log.debug("GET /api/products completed - thread: {}, elapsed: {}ms",
                            threadName, elapsedMs);
                });
    }

    /**
     * GET /api/stream
     * Server-Sent Events endpoint that streams 20 products with 200ms delay between each
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Product> streamProducts() {
        long startNano = System.nanoTime();
        String threadName = Thread.currentThread().getName();

        return Flux.range(1, 20)
                .map(id -> new Product((long) id, "Streamed Product " + id,
                        BigDecimal.valueOf(20.0 + id), "Stream description " + id))
                .delayElements(Duration.ofMillis(200))
                .doOnSubscribe(subscription ->
                    log.debug("Stream /api/products/stream started - thread: {}", threadName))
                .doOnComplete(() -> {
                    long elapsedMs = (System.nanoTime() - startNano) / 1_000_000;
                    log.debug("Stream /api/products/stream completed - thread: {}, elapsed: {}ms",
                            threadName, elapsedMs);
                });
    }
}
