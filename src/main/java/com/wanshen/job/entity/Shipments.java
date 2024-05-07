package com.wanshen.job.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class Shipments {
   private String shipmentId ;
   private Integer orderId ;
   private String origin ;
   private String destination ;
   private boolean  Arrived ;

}

//implements Serializable
