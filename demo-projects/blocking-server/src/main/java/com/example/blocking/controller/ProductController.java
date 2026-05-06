package com.example.blocking.controller;

import com.example.blocking.model.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/products")
public class ProductController {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @GetMapping("/{id}")
    public Product getProduct(@PathVariable Long id) {
        long startNano = System.nanoTime();
        String threadName = Thread.currentThread().getName();

        try {
            // Simulate 500ms DB delay
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long elapsedMs = (System.nanoTime() - startNano) / 1_000_000;
        log.info("GET /api/products/{} - Thread: {}, Elapsed: {}ms", id, threadName, elapsedMs);

        return new Product(
                id,
                "Product " + id,
                BigDecimal.valueOf(10.0 + id),
                "Description for product " + id
        );
    }

    @GetMapping
    public List<Product> getAllProducts() {
        long startNano = System.nanoTime();
        String threadName = Thread.currentThread().getName();
        List<Product> products = new ArrayList<>();

        for (int i = 1; i <= 10; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            products.add(new Product(
                    (long) i,
                    "Product " + i,
                    BigDecimal.valueOf(10.0 + i),
                    "Description for product " + i
            ));
        }

        long elapsedMs = (System.nanoTime() - startNano) / 1_000_000;
        log.info("GET /api/products - Thread: {}, Items: 10, Total Elapsed: {}ms", threadName, elapsedMs);

        return products;
    }

    /**
     * Synchronous SSE — blocks the Tomcat thread for the entire stream duration (~4s).
     * No executor, no background thread. Pure blocking I/O on the request thread.
     * With 200 default Tomcat threads, only 200 streams can run at once.
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public void streamProducts(HttpServletResponse response) throws IOException {
        long startNano = System.nanoTime();
        String threadName = Thread.currentThread().getName();

        response.setContentType("text/event-stream");
        response.setCharacterEncoding("UTF-8");
        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("Connection", "keep-alive");

        PrintWriter writer = response.getWriter();

        for (int i = 1; i <= 20; i++) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            Product product = new Product(
                    (long) i,
                    "Product " + i,
                    BigDecimal.valueOf(10.0 + i),
                    "Description for product " + i
            );

            String json = objectMapper.writeValueAsString(product);
            writer.write("data:" + json + "\n\n");
            writer.flush();

            if (writer.checkError()) {
                break; // client disconnected
            }
        }

        long totalElapsed = (System.nanoTime() - startNano) / 1_000_000;
        log.info("GET /api/products/stream - Thread: {}, Elapsed: {}ms, Items: 20",
                threadName, totalElapsed);
    }
}
