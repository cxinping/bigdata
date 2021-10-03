package com.dxtd.model;

public class CommonTest {
    public static void testLombok(){

        ProductOrder productOrder = new ProductOrder();
        productOrder.setId(1);
        productOrder.setTitle("微服务课程 flink");
        productOrder.setAmount(100l);

        int id = productOrder.getId();
        System.out.println(id);
        System.out.println(productOrder);
    }

    public static void main(String[] args) {
        testLombok();
    }

}
