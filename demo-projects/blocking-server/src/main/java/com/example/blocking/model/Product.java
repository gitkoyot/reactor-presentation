package com.example.blocking.model;

import java.math.BigDecimal;

public record Product(
        Long id,
        String name,
        BigDecimal price,
        String description
) {}
