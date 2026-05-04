package com.example.reactive.controller;

import com.example.reactive.model.Product;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;

@Slf4j
@WebFluxTest(ProductController.class)
@DisplayName("ProductController Tests")
class ProductControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private ProductController productController;

    @Test
    @DisplayName("Should retrieve single product with StepVerifier")
    void testGetProductById_WithStepVerifier() {
        Long productId = 1L;

        StepVerifier.create(productController.getProductById(productId))
                .assertNext(product -> {
                    assert product.id().equals(productId);
                    assert product.name().equals("Product 1");
                    assert product.price().equals(BigDecimal.valueOf(11.0));
                    assert product.description().equals("Description for product 1");
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should verify delay of 500ms for single product")
    void testGetProductById_WithVirtualTime() {
        Long productId = 2L;

        StepVerifier.withVirtualTime(() -> productController.getProductById(productId))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(500))
                .assertNext(product -> {
                    assert product.id().equals(productId);
                    assert product.name().equals("Product 2");
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Should verify time passage with VirtualTimeScheduler")
    void testGetProductById_VirtualTimeSchedulerVerification() {
            Mono<Product> productMono = productController.getProductById(3L)
                    .doOnSubscribe(s -> log.info("Subscribed at: {}", System.currentTimeMillis()));

            StepVerifier.withVirtualTime(() -> productMono)
                    .expectSubscription()
                    .expectNoEvent(Duration.ofMillis(500))
                    .assertNext(product -> {
                        assert product.id().equals(3L);
                        assert product.price().equals(BigDecimal.valueOf(13.0));
                    })
                    .verifyComplete();


    }

    @Test
    @DisplayName("Should get all products as flux")
    void testGetAllProducts() {
        StepVerifier.create(productController.getAllProducts())
                .expectNextCount(10)
                .verifyComplete();
    }

    @Test
    @DisplayName("Should stream products with SSE")
    void testStreamProducts() {
        StepVerifier.create(productController.streamProducts())
                .expectNextCount(20)
                .verifyComplete();
    }

    @Test
    @DisplayName("Should verify product structure in single product call")
    void testGetProductById_WebTestClient() {
        webTestClient.get()
                .uri("/api/products/5")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Product.class)
                .consumeWith(result -> {
                    Product product = result.getResponseBody();
                    assert product != null;
                    assert product.id().equals(5L);
                    assert product.name().equals("Product 5");
                });
    }

    @Test
    @DisplayName("Should verify Flux with virtual time and timing")
    void testGetAllProducts_WithTimingVerification() {
        StepVerifier.withVirtualTime(() -> productController.getAllProducts())
                .expectSubscription()
                .thenAwait(Duration.ofMillis(100)) // First product after 100ms
                .expectNextCount(1)
                .thenAwait(Duration.ofMillis(900)) // Remaining 9 products (9 * 100ms = 900ms)
                .expectNextCount(9)
                .verifyComplete();
    }

    @Test
    @DisplayName("Should verify single product is captured before delay completes")
    void testGetProductById_VerifyBeforeDelay() {
        Long productId = 10L;

        StepVerifier.withVirtualTime(() -> productController.getProductById(productId))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(499))
                .expectNoEvent(Duration.ofMillis(1))
                .assertNext(product -> {
                    assert product.id().equals(productId);
                })
                .verifyComplete();
    }
}
