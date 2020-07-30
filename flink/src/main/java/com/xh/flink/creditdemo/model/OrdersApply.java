package com.xh.flink.creditdemo.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrdersApply implements Serializable {


    private String user_id;

    private String product;

    private Double amount;

    private String ts;

}
