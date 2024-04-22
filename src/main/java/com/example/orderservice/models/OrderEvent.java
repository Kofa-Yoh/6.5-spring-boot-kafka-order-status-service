package com.example.orderservice.models;

import lombok.Data;

@Data
public class OrderEvent {
    private String product;
    private Integer quantity;
}
