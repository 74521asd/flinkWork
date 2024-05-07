package com.wanshen.job.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DruidUtils {
    //提供方法
    //供静态代码块加载配置文件，初始化连接池对象
    private static DataSource ds;

    static {
        //1.加载配置文件
        try {
            Properties pro = new Properties();
            pro.load(DruidUtils.class.getClassLoader().getResourceAsStream("druid.properties"));
            //2.获取datasource
            ds = DruidDataSourceFactory.createDataSource(pro);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //1.获取连接方法：通过数据库连接池获取连接
    public static Connection getConnection() throws SQLException, SQLException {
        return ds.getConnection();
    }

    //2.释放资源
    public static void close(Statement stmt, Connection conn) {
        close(stmt, null, conn);
    }

    public static void close(Statement stmt, ResultSet rs, Connection conn) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    //3.获取连接池的方法
    public static DataSource getDataSource() {
        return ds;
    }

}