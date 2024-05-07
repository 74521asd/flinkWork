package com.wanshen.job.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.sql.Date;


@NoArgsConstructor
@AllArgsConstructor
@Data
public class Orders {
  private int orderId;
  private Date orderDate ;
  private String customerName ;
  private BigDecimal price;
  private Integer productId ;
  private boolean orderStatus;
  private long createTime ;

  //  private int order_id;
  //  private Date order_date ;
  //  private String customer_name ;
  //  private BigDecimal price;
  //  private Integer product_id ;
  //  private boolean order_status;
}
