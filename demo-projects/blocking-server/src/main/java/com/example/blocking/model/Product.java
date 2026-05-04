package com.example.blocking.model;

public record Product(
        Long id,
        String name,
        Double price,
        String description
) {}
