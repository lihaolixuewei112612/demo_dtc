package com.dtc.java.analytic.V2.process.function;

import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created on 2020-02-21
 *
 * @author :hao.li
 */
@Slf4j
public class WinProcessMapFunction extends ProcessWindowFunction<DataStruct, DataStruct, Tuple, TimeWindow> {

    //磁盘描述
    private Map<String, String> diskDescribe = new HashMap();
    //磁盘每个块的大小
    private Map<String, String> diskBlockSize = new HashMap();
    //磁盘块的个数
    private Map<String, String> diskBlockNum = new HashMap();
    //磁盘容量
    private Map<String, String> diskCaption = new HashMap();
    //cpu个数
    private Map<String, String> cpuNum = new HashMap<>();
    private boolean flag = false;

    @Override
    public void process(Tuple tuple, Context context, Iterable<DataStruct> iterable, Collector<DataStruct> collector) throws Exception {
        ParameterTool parameters = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        String userName = parameters.get(PropertiesConstants.MYSQL_USERNAME);
        String passWord = parameters.get(PropertiesConstants.MYSQL_PASSWORD);
        String host = parameters.get(PropertiesConstants.MYSQL_HOST);
        String port = parameters.get(PropertiesConstants.MYSQL_PORT);
        String database = parameters.get(PropertiesConstants.MYSQL_DATABASE);
        String mysql_win_table = parameters.get(PropertiesConstants.MYSQL_WINDOWS_TABLE);
        double sum = 0;
        for (DataStruct wc : iterable) {
            String keyValue = wc.getHost() + "_" + wc.getZbLastCode();
            /**
             * cpu使用率
             * */
            if ("101_100_101_101_101".equals(wc.getZbFourName())) {
                if (!cpuNum.containsKey(wc.getZbLastCode())) {
                    cpuNum.put(wc.getZbLastCode(), "1");
                } else {
                    flag = true;
                }
            }
            if (flag) {
                if ("101_100_101_101_101".equals(wc.getZbFourName())) {
                    sum += Double.parseDouble(wc.getValue());
                }
                double result = sum / cpuNum.size();
                collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), wc.getZbFourName(), wc.getZbLastCode(), wc.getNameCN(), wc.getNameEN(), wc.getTime(), String.valueOf(result)));
            }
            /**
             * 磁盘描述
             * */
            if ("101_100_103_103_103".equals(wc.getZbFourName())) {
                if ((!diskDescribe.containsKey(keyValue)) || (diskDescribe.containsKey(keyValue) && (!diskDescribe.get(keyValue).equals(wc.getValue())))) {
                    diskDescribe.put(keyValue, wc.getValue());
                    String Url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
                    String jdbcName = "com.mysql.jdbc.Driver";
                    String sql = "insert into" + mysql_win_table + "values(?,?,?)";
                    Connection con = null;
                    try {
                        Class.forName(jdbcName);//向DriverManager注册自己
                        con = DriverManager.getConnection(Url, userName, passWord);//与数据库建立连接
                        PreparedStatement pst = con.prepareStatement(sql);//用来执行SQL语句查询，对sql语句进行预编译处理
                        pst.setString(1, wc.getHost());
                        pst.setString(2, wc.getZbLastCode());
                        pst.setString(3, wc.getValue());
                        pst.executeUpdate();
                    } catch (ClassNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } finally {
                        if (con != null) {
                            con.close();
                        }

                    }
                }
            }
            /**
             * 每个块的大小
             * */
            if ("101_100_103_104_104".equals(wc.getZbFourName())) {
                if (!diskBlockSize.containsKey(keyValue)) {
                    diskBlockSize.put(keyValue, wc.getValue());
                }
            }
            /**
             * 磁盘块的个数
             * */
            if ("101_100_103_104_105".equals(wc.getZbFourName())) {
                if (!diskBlockNum.containsKey(keyValue)) {
                    diskBlockNum.put(keyValue, wc.getValue());
                }
            }


            /**
             * 每个盘的总容量
             * */
            if (getLikeByMap(diskBlockNum, wc.getHost()) == getLikeByMap(diskBlockSize, wc.getHost())) {
                for (String keyA : diskBlockSize.keySet()) {
                    for (String keyB : diskBlockNum.keySet()) {
                        if (keyA.equals(keyB)) {
                            double valueA = Double.parseDouble(diskBlockNum.get(keyA));
                            double valueB = Double.parseDouble(diskBlockSize.get(keyB));
                            if (!diskCaption.containsKey(keyA)) {
                                diskCaption.put(keyA, String.valueOf(valueA * valueB));
                            }
                        }
                    }
                }
            }
            /**
             * 磁盘使用量
             * 磁盘使用率
             * 虚拟/物理内存使用率
             * */
            if ("101_100_103_104_106".equals(wc.getZbFourName()) && diskCaption.containsKey(keyValue)) {
                Double resu = Double.parseDouble(wc.getValue()) * Double.parseDouble(diskBlockSize.get(keyValue));
                Double diskUsedCapacity = Double.parseDouble(diskCaption.get(keyValue));
                //磁盘使用率
                Double result = resu / diskUsedCapacity;
                collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), wc.getZbFourName() + "." + wc.getZbLastCode(), wc.getZbLastCode(), wc.getNameCN(), wc.getNameEN(), wc.getTime(), String.valueOf(result)));
                continue;
            }
        }
    }

    private int getLikeByMap(Map<String, String> map, String keyLike) {
        List<String> list = new ArrayList<>();
        for (Map.Entry<String, String> entity : map.entrySet()) {
            if (entity.getKey().startsWith(keyLike)) {
                list.add(entity.getValue());
            }
        }
        return list.size();
    }
}
