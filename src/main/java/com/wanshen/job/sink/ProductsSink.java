package com.wanshen.job.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.wanshen.job.common.Config;
import com.wanshen.job.entity.Products;
import com.wanshen.job.util.DruidUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.ws.rs.DELETE;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;


import static com.wanshen.job.common.Config.INSERT;
import static com.wanshen.job.common.Config.UPDATE;

@Slf4j
public class ProductsSink extends RichSinkFunction<JSONObject> {

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
        Products products = JSON.parseObject(param.toJSONString(), Products.class);

        int id = products.getId();
        String name = products.getName();
        String description = products.getDescription();


        String operType = param.getString("MARK");
        try {

            ps = conn.prepareStatement(sql);
//UPDATE products SET create_date = ?, name = ?,description=? where id = ?
            if (UPDATE.equals(operType)){
                log.info("执行:UPDATE");
              //  ps.setDate(1,new java.sql.Date(new Date().getTime()));
                ps.setString(1,name);
                ps.setString(2,description);
                ps.setInt(3,id);

            }
            //insert into products (id,create_date,name,description)values(?,?,?,?)
            if (INSERT.equals(operType)){
                log.info("执行:INSERT");
                ps.setInt(1,id);
                ps.setDate(2,new java.sql.Date(new Date().getTime()));
                ps.setString(3,name);
                ps.setString(4,description);
            }
            //delete from products  where id =?
            if (Config.DELETE.equals(operType)){
                log.info("执行:DELETE");
                ps.setInt(1,id);
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
