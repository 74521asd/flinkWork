package com.wanshen.job.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class Products {
   private int id ;
   private String name ;
   private String description ;
   private long createTime ;

}
