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
            String sql = "select g.*,h.id,h.`name` from (select e.*,f.is_enable,f.alarm_level from (select * from (select a.strategy_id,a.asset_id,b.trigger_name,b.comparator,b.number,b.`code`,\n" +
                    "c.ipv4 from strategy_asset_mapping a left join strategy_trigger b on a.strategy_id = b.strategy_id left join asset c on c.id = a.asset_id) \n" +
                    "d where d.ipv4!=\"\" and d.`code`!=\"\") e left join alarm_strategy f on e.strategy_id = f.id) g left join asset_indice h on g.`code`=h.`code`";
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
