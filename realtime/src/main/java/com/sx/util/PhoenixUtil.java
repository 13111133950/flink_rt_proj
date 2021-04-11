package com.sx.util;

import com.sx.common.GlobalConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName PhoenixUtil
 * @Author Kurisu
 * @Description
 * @Date 2021-3-6 18:30
 * @Version 1.0
 **/
public class PhoenixUtil {
    private static Connection conn = null;

    public static void init(){
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GlobalConfig.PHOENIX_SERVER);
            conn.setSchema(GlobalConfig.HBASE_SCHEMA);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static<T> List<T> queryList(String sql,Class<T> clz){
        if(conn==null){
            init();
        }
        ArrayList<T> resultList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                T rowData = clz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    //TODO 使用BeanUtils为对象赋值  封装成的类字段顺序和名称与数据库一致
                    BeanUtils.setProperty(rowData, metaData.getColumnName(i), rs.getObject(i));
                }
                resultList.add(rowData);
            }
            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }
}
