package com.wanshen.job.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wanshen.job.common.Config;
import com.wanshen.job.entity.Shipments;
import com.wanshen.job.util.DruidUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import static com.wanshen.job.common.Config.INSERT;
import static com.wanshen.job.common.Config.UPDATE;

@Slf4j
public class ShipmentsSink extends RichSinkFunction<JSONObject> {

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
        Shipments shipments = JSON.parseObject(param.toJSONString(), Shipments.class);
        String shipment_id = shipments.getShipmentId();
        int order_id = shipments.getOrderId();
        String destination = shipments.getDestination();
        String origin = shipments.getOrigin();
        boolean is_arrived = shipments.isArrived();


        String operType = param.getString("MARK");
        try {

            ps = conn.prepareStatement(sql);
//UPDATE Shipments SET  order_id = ?,destination=?,origin= ?,is_arrived= ? where shipment_id = ?
            if (UPDATE.equals(operType)){
                log.info("执行:UPDATE");
                //ps.setDate(1,new java.sql.Date(new Date().getTime()));
                ps.setInt(1,order_id);
                ps.setString(2,destination);
                ps.setString(3,origin);
                ps.setBoolean(4,is_arrived);
                ps.setString(5,shipment_id);

            }
            //insert into Shipments (shipment_id,create_date,order_id,destination,origin,is_arrived)values(?,?,?,?,?,?)
            if (INSERT.equals(operType)){
                log.info("执行:INSERT");
                ps.setString(1,shipment_id);
                ps.setDate(2,new java.sql.Date(new Date().getTime()));
                ps.setInt(3,order_id);
                ps.setString(4,destination);
                ps.setString(5,origin);
                ps.setBoolean(6,is_arrived);


            }
            //delete from Shipments  where shipment_id =?
            if (Config.DELETE.equals(operType)){
                log.info("执行:DELETE");
                ps.setString(1,shipment_id);
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
