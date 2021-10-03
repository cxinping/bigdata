package com.dxtd.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VideoOrder {
    private String tradeNo;
    private String title;
    private int money;



}