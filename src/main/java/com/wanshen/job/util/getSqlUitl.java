package com.wanshen.job.util;

import com.alibaba.fastjson.JSONObject;
import com.wanshen.job.common.Config;

import java.sql.SQLException;
import java.util.Map;

import static com.wanshen.job.common.Tools.*;

public class getSqlUitl {
    public static  String getSinkSql(JSONObject param, String tableName,Map<String,String> mapping,String mark,String condition,String bean) throws SQLException, NoSuchFieldException, ClassNotFoundException {
        String SQL="";
        if (Config.INSERT.equals(mark)) {
            SQL = getInsertSql(param, tableName, bean, mapping);
        }        if (Config.DELETE.equals(mark)) {
            SQL = getDeleteSql(param, tableName,condition, bean, mapping);
        }        if (Config.UPDATE.equals(mark)) {
            SQL = getUpdateSql(param, tableName,condition, bean, mapping);
        }
        return SQL;
    }

    public static  String getSinkSql(JSONObject param, String tableName,Map<String,String> mapping,String mark,String condition,Map<String,String> types) throws SQLException, NoSuchFieldException, ClassNotFoundException {
        String SQL="";
        if (Config.INSERT.equals(mark)) {
            SQL = getInsertSql(param, tableName, types, mapping);
        }        if (Config.DELETE.equals(mark)) {
            SQL = getDeleteSql(param, tableName,condition, types, mapping);
        }        if (Config.UPDATE.equals(mark)) {
            SQL = getUpdateSql(param, tableName,condition, types, mapping);
        }
        return SQL;
    }
}
