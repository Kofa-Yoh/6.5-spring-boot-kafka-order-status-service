package com.example.orderservice.models;

import lombok.Data;

@Data
public class Order {
    private String product;
    private Integer quantity;
}
