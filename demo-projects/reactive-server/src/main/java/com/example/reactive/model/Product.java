package com.example.reactive.model;

import java.math.BigDecimal;

public record Product(
        Long id,
        String name,
        BigDecimal price,
        String description
) {
    public static Product create(Long id, String name, BigDecimal price, String description) {
        return new Product(id, name, price, description);
    }
}
