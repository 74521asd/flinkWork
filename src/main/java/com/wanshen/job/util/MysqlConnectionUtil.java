package com.wanshen.job.util;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

@Slf4j
public class MysqlConnectionUtil {
   private static Connection conn =null;

    public static Connection getConnection(String url,String username,String password){


        try{
            //数据库驱动字符串
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(url,username,password);
            //判断是否成功连接
            if (null!=conn){
                log.info("获取MySQL连接成功！Connection successful");
            }else{
                log.error("获取MySQL连接失败！Connection ERROR");
            }
        }catch (Exception e){
            e.printStackTrace();   //处理异常
            log.error("获取链接异常！！"+e);
        }
        return conn;

    }

    public static void close(ResultSet rs, PreparedStatement pre, Connection conn) {
        //若数据库连接对象不为空，否则关闭
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //若PrepareStatement对象不为空，否则关闭
        if (pre != null) {
            try {
                pre.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //若结果集对象不为空，否则关闭
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
//        Connection connection = MysqlConnectionUtil.getConnection("jdbc:mysql://localhost:3306/jeecg-boot?useUnicode=true&characterEncoding=utf8&useSSL=false",
//                "root",
//                "root"
//        );
//
//        //serverTimezone=Shanghai&?
//        System.out.println("connection = " + connection);

//        Connection   connection = MysqlConnectionUtil.getConnection(
//                "jdbc:mysql://10.80.59.63:3306/datahub?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=true",
//                "datahub",
//                "!Q@W3e4r"
//        );
//        System.out.println("connection = " + connection);

//        Connection   connection = MysqlConnectionUtil.getConnection(
//                "jdbc:mysql://10.80.79.20:3306/demo?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=true",
//                "obei",
//                "!WAX2qsz"
//        );
//        System.out.println("connection = " + connection);

        Connection   connection = MysqlConnectionUtil.getConnection(
                "jdbc:mysql://localhost:3306/demo?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT",
                "root",
                "root"
        );
        System.out.println("connection = " + connection);

    }


}
