package com.dtc.java.analytic.V2.source.mysql;


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
 * Created on 2019-12-30
 *
 * @author :hao.li
 */
@Slf4j
public class GetAlarmNotifyData extends RichSourceFunction<Map<String, String>> {

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
            String sql = "select b.asset_id,e.ipv4,c.strategy_id,c.trigger_kind,c.trigger_name,c.number,c.unit,c.`code`,d.is_enable,d.alarm_level,d.up_time from (select * from strategy_trigger a \n" +
                    "where a.code!=\"\" and a.comparator='>') c left join strategy_asset_mapping b on c.strategy_id = b. strategy_id left join alarm_strategy d on c.strategy_id = d.id left join asset e on e.id = b.asset_id";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Map<String, String>> ctx) throws Exception {
        Map<String, String> map = new HashMap<>();
        Tuple4<String, String, Short, String> test = null;
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                if ("true".equals(resultSet.getString("used"))) {
                    String unique_id = resultSet.getString("unique_id");
                    String ip = resultSet.getString("host_ip");
                    String code = resultSet.getString("items_code");
                    String alarm = resultSet.getString("alarm_threshold_garde");
                    String key = ip + "." + code.replace("_",".");
                    String result = unique_id + ":" + code + ":" + alarm;
                    map.put(key, result);
                }
            }
            log.info("=======select alarm notify from mysql, size = {}, map = {}", map.size(), map);
            ctx.collect(map);
            map.clear();
            Thread.sleep(1000 * 60);
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
