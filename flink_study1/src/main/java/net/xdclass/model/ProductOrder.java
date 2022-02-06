package net.xdclass.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *
 *
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductOrder {
    private int id;

    private String title;

    private Long amount;

}


