package com.wanshen.job.common;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.streamload.shade.com.alibaba.fastjson.JSON;
import com.wanshen.job.entity.Orders;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.beans.IntrospectionException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 工具方法类
 */
@Slf4j
public class Tools {
    public static void main(String[] args) throws Exception {
        Orders orders = new Orders();
        orders.setOrderId(1);
        Date date = new Date(new java.util.Date().getTime());
        orders.setOrderDate(date);
        Date orderDate = orders.getOrderDate();
        System.out.println("orderDate = " + orderDate);
        orders.setOrderStatus(true);
        orders.setCustomerName("test");
        orders.setPrice(new BigDecimal(555));
        orders.setProductId(5);
        String s = JSON.toJSONString(orders);
        JSONObject jsonObject = JSONObject.parseObject(s);
        HashMap<String, String> map = new HashMap<>();
        map.put("orderStatus","order_status");
        map.put("orderId","order_id");
        map.put("orderDate","order_date");
        map.put("customerName","customer_name");


//        String orders1 = getUpdateSql(jsonObject, "orders", "orderId,productId", "com.wanshen.job.entity.Orders",map);
//        System.out.println("orders1 = " + orders1);
//        String orders2 = getInsertSql(jsonObject, "orders", "com.wanshen.job.entity.Orders", map);
//        System.out.println("orders2 = " + orders2);
//        String orders3 = getDeleteSql(jsonObject, "orders", "orderId,productId", "com.wanshen.job.entity.Orders", map);
//        System.out.println("orders3 = " + orders3);

        Row row = new Row(3);

        row.setField(0,3);
        row.setField(1,"ddd");
        row.setField(2,65);
        JSONObject jsonObject1 = rowToJsonObject(row);
        System.out.println("jsonObject1 = " + jsonObject1);


    }

    /**
     * 通过json对象转换成对应的
     * @param param
     * @param tableName
     * @param condition
     * @param types
     * @param mapping
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws NoSuchFieldException
     */
    public static String getDeleteSql(JSONObject param, String tableName, String condition, Map<String,String> types, Map<String,String> mapping) throws ClassNotFoundException, SQLException, NoSuchFieldException {
        //todo 映射， 如果有部分字段在表和实体类之间有区别，可以填写映射字段
        StringBuffer buffer = new StringBuffer();
        StringBuffer buffer2 = new StringBuffer();

        //delete from Orders  where order_id =?
        if ( StringUtils.isBlank(tableName)) throw new RuntimeException("sql或bean运行参数缺失！！");
        buffer.append("delete from "+ tableName+" ");
        String[] split = condition.split(",");
        buffer.append(" where 1=1 ");
        String condi =" where 1=1 ";

        for (String key : split){
            String simpleName = types.get(key);
            Object value = param.get(key);
            if (mapping.containsKey(key)) key= mapping.get(key).toString();

            if (StringUtils.isNotBlank(String.valueOf(value))) {
                if ("String".equals(simpleName)) {
                    buffer.append(" and " + key + " = '" + value + "'") ;
                } else if ("Date".equals(simpleName)){
                    buffer.append(" and " + key + " = '" + param.getSqlDate(key) + "'") ;
                } else {
                    buffer.append(" and " + key + " =" + value);
                }
                param.remove(key);
            }

        }
        return buffer.toString();

    }

    public static String getDeleteSql(JSONObject param, String tableName, String condition, String bean, Map mapping) throws ClassNotFoundException, SQLException, NoSuchFieldException {
        //todo 映射， 如果有部分字段在表和实体类之间有区别，可以填写映射字段
        StringBuffer buffer = new StringBuffer();
        StringBuffer buffer2 = new StringBuffer();
        Class<?> aClass = Class.forName(bean);
        //delete from Orders  where order_id =?
        if (StringUtils.isBlank(bean) || StringUtils.isBlank(tableName)) throw new RuntimeException("sql或bean运行参数缺失！！");
        buffer.append("delete from "+ tableName+" ");
        String[] split = condition.split(",");
        buffer.append(" where 1=1 ");
        String condi =" where 1=1 ";

        for (String c : split){
            String simpleName = aClass.getDeclaredField(c).getType().getSimpleName();
            Object value = param.get(c);
            if (mapping.containsKey(c)) c= mapping.get(c).toString();

            if (StringUtils.isNotBlank(String.valueOf(value))) {
                if ("String".equals(simpleName)) {
                    buffer.append(" and " + c + " = '" + value + "'") ;
                } else if ("Date".equals(simpleName)){
                    buffer.append(" and " + c + " = '" + param.getSqlDate(c) + "'") ;
                } else {
                    buffer.append(" and " + c + " =" + value);
                }
                param.remove(c);
            }

        }
        return buffer.toString();

    }

    public static String getInsertSql(JSONObject param, String tableName,Map<String,String> types, Map<String,String> mapping) throws ClassNotFoundException, SQLException, NoSuchFieldException {
        //todo 映射， 如果有部分字段在表和实体类之间有区别，可以填写映射字段
        StringBuffer buffer = new StringBuffer();
        StringBuffer buffer2 = new StringBuffer();

        //insert into Orders (order_id,order_date,customer_name,price,product_id,order_status)values(?,?,?,?,?,?)
        if ( StringUtils.isBlank(tableName)) throw new RuntimeException("sql或bean运行参数缺失！！");
        buffer.append("insert into "+ tableName+" ( ");

        Set<String> keys = param.keySet();
        for (String key : keys){
            System.out.println("key = " + key);
            String colum="";
            if (mapping.containsKey(key)) { colum= mapping.get(key).toString();}
            else{ colum=key;}
            buffer.append(colum+",");
            //通过字段名称从map集合里面取出对应的字段类型
            String simpleName = types.get(key);

            Object value = param.get(key);
            if (StringUtils.isNotBlank(String.valueOf(value))) {
                if ("String".equals(simpleName)) {
                    buffer2.append(" '" + param.getString(key) + "',");
                } else if ("Date".equals(simpleName)) {
                    buffer2.append(" '" + param.getSqlDate(key) + "',");
                } else {
                    buffer2.append(param.getString(key) + ", ");
                }
            }

        }
        buffer.deleteCharAt(buffer.length()-1);
        buffer.append(" )values( ");

        buffer2.deleteCharAt(buffer2.length()-1);
        buffer2.append(")");
        buffer.append(buffer2);
        return buffer.toString();

    }

    public static String getInsertSql(JSONObject param, String tableName, String bean, Map<String,String> mapping) throws ClassNotFoundException, SQLException, NoSuchFieldException {
        //todo 映射， 如果有部分字段在表和实体类之间有区别，可以填写映射字段
        StringBuffer buffer = new StringBuffer();
        StringBuffer buffer2 = new StringBuffer();
        Class<?> aClass = Class.forName(bean);
        //insert into Orders (order_id,order_date,customer_name,price,product_id,order_status)values(?,?,?,?,?,?)
        if (StringUtils.isBlank(bean) || StringUtils.isBlank(tableName)) throw new RuntimeException("sql或bean运行参数缺失！！");
       buffer.append("insert into "+ tableName+" ( ");

        Set<String> keys = param.keySet();
        for (String key : keys){
            System.out.println("key = " + key);
            String colum="";
            if (mapping.containsKey(key)) { colum= mapping.get(key).toString();}
               else{ colum=key;}
            buffer.append(colum+",");

            String simpleName = aClass.getDeclaredField(key).getType().getSimpleName();

            Object value = param.get(key);
            if (StringUtils.isNotBlank(String.valueOf(value))) {
                if ("String".equals(simpleName)) {
                    buffer2.append(" '" + param.getString(key) + "',");
                } else if ("Date".equals(simpleName)) {
                    buffer2.append(" '" + param.getSqlDate(key) + "',");
                } else {
                    buffer2.append(param.getString(key) + ", ");
                }
            }

        }
     buffer.deleteCharAt(buffer.length()-1);
        buffer.append(" )values( ");

        buffer2.deleteCharAt(buffer2.length()-1);
        buffer2.append(")");
        buffer.append(buffer2);
        return buffer.toString();

    }

    public static String getUpdateSql(JSONObject param, String tableName, String condition, Map<String,String> types, Map<String,String> mapping) throws ClassNotFoundException, SQLException, NoSuchFieldException {
        //todo 映射， 如果有部分字段在表和实体类之间有区别，可以填写映射字段
        StringBuffer buffer = new StringBuffer();
        //UPDATE Orders SET price = ?, customer_name = ?,product_id=?, order_status = ? where order_id = ?
        if ( StringUtils.isBlank(tableName)) throw new RuntimeException("sql或bean运行参数缺失！！");
        String condi =" where 1=1 ";
        String[] split = condition.split(",");
        for (String c : split){
            String simpleName = types.get(c);
            Object value = param.get(c);

            if (mapping.containsKey(c)) c= mapping.get(c).toString();

            if (StringUtils.isNotBlank(String.valueOf(value))) {
                if ("String".equals(simpleName)) {
                    condi = condi + " and " + c + " = '" + value + "'";
                } else {
                    condi = condi + " and " + c + " =" + value;
                }

                param.remove(c);
            }

        }

        buffer.append("UPDATE "+tableName+" set ");
        Set<String> keys = param.keySet();
        for (String key : keys){
            String simpleName = types.get(key);
            if (mapping.containsKey(key)) key= mapping.get(key).toString();
            if ("String".equals(simpleName)) {
                buffer.append(key + "= '" + param.getString(key) + "',");
            }else {
                buffer.append(key+"="+param.getString(key)+", ");
            }
        }
        String sql = buffer.substring(0, buffer.length() - 1).toString();
        String updateSql=sql+condi;

        return updateSql;

    }

    public static String getUpdateSql(JSONObject param, String tableName, String condition, String bean, Map<String,String> mapping) throws ClassNotFoundException, SQLException, NoSuchFieldException {
       //todo 映射， 如果有部分字段在表和实体类之间有区别，可以填写映射字段
        StringBuffer buffer = new StringBuffer();
        Class<?> aClass = Class.forName(bean);
        //UPDATE Orders SET price = ?, customer_name = ?,product_id=?, order_status = ? where order_id = ?
        if (StringUtils.isBlank(bean) || StringUtils.isBlank(tableName)) throw new RuntimeException("sql或bean运行参数缺失！！");
        String condi =" where 1=1 ";
        String[] split = condition.split(",");
        for (String c : split){
            String simpleName = aClass.getDeclaredField(c).getType().getSimpleName();
            Object value = param.get(c);

          if (mapping.containsKey(c)) c= mapping.get(c).toString();

            if (StringUtils.isNotBlank(String.valueOf(value))) {
                if ("String".equals(simpleName)) {
                    condi = condi + " and " + c + " = '" + value + "'";
                } else {
                    condi = condi + " and " + c + " =" + value;
                }

                param.remove(c);
            }

        }

        buffer.append("UPDATE "+tableName+" set ");
        Set<String> keys = param.keySet();
        for (String key : keys){
            String simpleName = aClass.getDeclaredField(key).getType().getSimpleName();
            if (mapping.containsKey(key)) key= mapping.get(key).toString();
            if ("String".equals(simpleName)) {
                buffer.append(key + "= '" + param.getString(key) + "',");
            }else {
                buffer.append(key+"="+param.getString(key)+", ");
            }
        }
        String sql = buffer.substring(0, buffer.length() - 1).toString();
        String updateSql=sql+condi;

        return updateSql;

    }

    public static void setParam(JSONObject param, PreparedStatement ps) throws ClassNotFoundException, SQLException {
        String bean = param.getString("bean");
        String SQL = param.getString("sql");
        if (StringUtils.isBlank(bean)||StringUtils.isBlank(bean)) throw new RuntimeException("sql或bean运行参数缺失！！");
        Class<?> aClass = Class.forName(bean);
        Field[] fields = aClass.getDeclaredFields();
        int i=0;
        for (int j = 0; j < fields.length; j++) {
            Object o = param.get(fields[j].getName());
            if (null !=o) {
                System.out.println("o = " + o);
                i++;
                fields[j].setAccessible(true);
                String name = fields[j].getName();
                String type = fields[j].getType().getSimpleName();
                log.info("type = " + type+"字段名="+name+"位置"+i);
                System.out.println("type=" + type+"字段名="+name+"位置="+i);
                switch (type) {
                    case "String":
                        ps.setString(i, param.getString(name));
                        break;
                    case "Long":
                    case "long":
                        ps.setLong(i, param.getLong(name));
                        break;
                    case "Date":
                        ps.setDate(i, new java.sql.Date(param.getDate(name).getTime()));
                        break;
                    case "BigDecimal":
                        ps.setBigDecimal(i, param.getBigDecimal(name));
                        break;
                    case "boolean":
                        ps.setBoolean(i, param.getBoolean(name));
                        break;
                    case "Integer":
                    case "int":
                        ps.setInt(i, param.getInteger(name));
                        break;
                    case "float":
                        ps.setFloat(i, param.getFloat(name));
                        break;
                }
            }

        }

    }


    public static void fullSql(JSONObject param, String name, String type, StringBuffer buffer) {
        switch (type) {
            case "String":
             buffer.append(name +"='"+ param.getString(name)+"',");
                break;
            case "Long":
            case "long":

                break;
            case "Date":

                break;
            case "BigDecimal":

                break;
            case "boolean":

                break;
            case "Integer":
            case "int":

                break;
            case "float":

                break;
        }
    }

    /**
     * flink ROW类型转jsonObject对象
     * @param row
     * @return
     */

    public static JSONObject rowToJsonObject(Row row){
        int arity = row.getArity();
        JSONObject jsonObject = new JSONObject();
       Set<String> fieldNames = row.getFieldNames(true);
        System.out.println("fieldNames.size() = " + fieldNames.size());
        Iterator<String> iterator = fieldNames.iterator();
        while (iterator.hasNext()){
            String key = iterator.next();
            Object value = row.getField(key);
            jsonObject.put(key,value);
        }
        return jsonObject ;
    }



}
