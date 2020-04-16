package com.dtc.java.SC.JaShiCang.source;


import com.dtc.java.SC.common.MySQLUtil;
import com.dtc.java.SC.common.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : 驾驶舱监控大盘--设备正常数
 */
@Slf4j
public class JSC_ZC_Used_Num extends RichSourceFunction<Tuple2<String,Integer>> {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
    private ParameterTool parameterTool;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        connection = MySQLUtil.getConnection(parameterTool);

        if (connection != null) {
//            String sql = "select count(*) as AllNum from asset a where a.room is not null and a.partitions is not null and a.box is not null";
//            String sql = "select count(*) as AllNum from asset a where a.id not in (select distinct asset_id from alarm WHERE TO_DAYS(time_occur) = TO_DAYS(NOW())) and a.room is not null";
            String sql = "select ifnull(z.`name`,'其他') as name,sum(num) as num from (select * from (select m.zc_name,m.parent_id as pd,count(*) as num from (select a.asset_id as a_id,c.parent_id,c.`name` as zc_name,b.`status` from asset_category_mapping a \n" +
                    "left join asset b on a.asset_id=b.id left join asset_category c on c.id = a.asset_category_id where b.`status`='0') m GROUP BY m.zc_name) x left join asset_category y on x.pd = y.id) z group by z.`name`";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple2<String,Integer>> ctx) throws Exception {
        int num = 0;
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                num = resultSet.getInt("num");
                ctx.collect(Tuple2.of(name,num));
            }
            Thread.sleep(1000 * 6);
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
