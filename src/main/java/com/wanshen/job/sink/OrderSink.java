package com.wanshen.job.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.wanshen.job.common.Config;
import com.wanshen.job.entity.Orders;
import com.wanshen.job.util.DruidUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.ws.rs.DELETE;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.wanshen.job.common.Config.INSERT;
import static com.wanshen.job.common.Config.UPDATE;


@Slf4j
public class OrderSink extends RichSinkFunction<JSONObject> {

    Connection conn = null;
    PreparedStatement ps = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        conn= DruidUtils.getConnection();

    }


    @Override
    public void invoke(JSONObject param, Context context) throws Exception {
        String sql = param.getString("sql");
        System.out.println("sql = " + sql);
        Orders orders = JSON.parseObject(param.toJSONString(), Orders.class);
        int order_id = orders.getOrderId();
        Date order_date = orders.getOrderDate();
        String customer_name = orders.getCustomerName();
        BigDecimal price = orders.getPrice();
        Integer product_id = orders.getProductId();
        boolean order_status = orders.isOrderStatus();
        System.out.println("order_id = " + order_id);
        String operType = param.getString("MARK");
        try {

            ps = conn.prepareStatement(sql);
//UPDATE Orders SET price = ?, customer_name = ?,product_id=?, order_status = ? where order_id = ?
            if (UPDATE.equals(operType)){
                log.info("执行:UPDATE");
                ps.setBigDecimal(1,price);
                ps.setString(2,customer_name);
                ps.setInt(3,product_id);
                ps.setBoolean(4,order_status);
                ps.setInt(5,order_id);
            }
            if (INSERT.equals(operType)){
                log.info("执行:INSERT");
                ps.setInt(1,order_id);
                ps.setDate(2,order_date);
                ps.setString(3,customer_name);
                ps.setBigDecimal(4,price);
                ps.setInt(5,product_id);
                ps.setBoolean(6,true);

            }
            if (Config.DELETE.equals(operType)){
                log.info("执行:DELETE");
                ps.setInt(1,order_id);
            }

            ps.execute();
        } catch (SQLException e) {
            log.error("SQL执行异常={}",e);
        }

    }




    @Override
    public void close() throws Exception {
      DruidUtils.close(ps,conn);
    }
}
