package com.dtc.java.shucang.JGPF;


import com.dtc.java.analytic.V1.alter.MySQLUtil;
import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : 各机房未关闭告警数
 */
@Slf4j
public class ReadDataZC_BZC extends RichSourceFunction<Map<Integer, String>> {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
    private ParameterTool parameterTool;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        String database = parameterTool.get(PropertiesConstants.MYSQL_DATABASE);
        String host = parameterTool.get(PropertiesConstants.MYSQL_HOST);
        String password = parameterTool.get(PropertiesConstants.MYSQL_PASSWORD);
        String port = parameterTool.get(PropertiesConstants.MYSQL_PORT);
        String username = parameterTool.get(PropertiesConstants.MYSQL_USERNAME);
        String alarm_rule_table = parameterTool.get(PropertiesConstants.MYSQL_ALAEM_TABLE);

        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = MySQLUtil.getConnection(driver, url, username, password);

        if (connection != null) {
           String sql = "select m.room,m.position,n.num as zc,(m.num-n.num) as bzc from (select a.room as room ,a.position as position,count(*) as num from asset a group by a.room,a.position having a.room is not null and a.position is not null) m left join (select b.room as room,b.position as position,count(*) as num from asset b where b.id not in (select distinct asset_id from alarm) group by b.room,b.position having b.room is not null and b.position is not null) n on m.room=n.room and m.position=n.position";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Map<Integer, String>> ctx) throws Exception {
        Map<Integer, String> map = new HashMap<>();
        Tuple4<String, String, Short, String> test = null;
        Integer id =0;
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
//            resultSet.last();
//            resultSet.getRow();
            while (resultSet.next()) {
                id = resultSet.getInt("num");
                String room = resultSet.getString("room");
                map.put(id, room);
            }
            log.info("=======select alarm notify from mysql, size = {}, map = {}", map.size(), map);
            ctx.collect(map);
            map.clear();
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            log.error("runException:{}", e);
        }
        isRunning = false;
    }
}
