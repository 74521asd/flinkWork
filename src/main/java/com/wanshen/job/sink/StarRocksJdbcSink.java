package com.wanshen.job.sink;

import com.alibaba.fastjson.JSONObject;
import com.wanshen.job.util.DruidUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.PreparedStatement;



@Slf4j
public class StarRocksJdbcSink extends RichSinkFunction<JSONObject> {

    Connection conn = null;
    PreparedStatement ps = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        conn= DruidUtils.getConnection();

    }


    @Override
    public void invoke(JSONObject param, Context context) throws Exception {
        String sql = param.getString("sql");
        log.info("执行的sql={}",sql);
        ps = conn.prepareStatement(sql);
        ps.execute();
    }




    @Override
    public void close() throws Exception {
      DruidUtils.close(ps,conn);
    }
}
