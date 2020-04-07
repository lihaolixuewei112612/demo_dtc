package com.dtc.java.SC.daping;

import com.dtc.java.SC.JFSBWGBGJ.ExecutionEnvUtil;
import com.dtc.java.SC.daping.source.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Properties;


/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : 数仓监控大盘指标总类
 */
public class test {

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        Map<String, String> stringStringMap = parameterTool.toMap();
        Properties properties = new Properties();
        for (String key : stringStringMap.keySet()) {
            if (key.startsWith("mysql")) {
                properties.setProperty(key, stringStringMap.get(key));
            }
        }
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        int windowSizeMillis = 6000;
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        /**各机房各区域各机柜设备总数*/
        //大盘今日监控设备数
        //DataStreamSource<Tuple2<Integer,Integer>> zsStream = env.addSource(new DaPingAllNum()).setParallelism(1);
        //大盘今日告警数
        //DataStreamSource<Tuple2<Integer, Integer>> tuple2DataStreamSource = env.addSource(new DaPingAlarm()).setParallelism(1);
        //大盘今日工单
        DataStreamSource<Tuple2<Integer, Integer>> tuple2DataStreamSource1 = env.addSource(new DaPingOrder()).setParallelism(1);
        //变更
        DataStreamSource<Tuple2<Integer, Integer>> tuple2DataStreamSource2 = env.addSource(new DaPingBianGengOrder()).setParallelism(1);
        //正常运行
        //DataStreamSource<Tuple2<Integer, Integer>> tuple2DataStreamSource3 = env.addSource(new DaPingZCAllNum()).setParallelism(1);
        //未处理告警数
        DataStreamSource<Tuple2<String,Integer>> DaPingWCLAlarm = env.addSource(new DaPingWCLAlarm()).setParallelism(1);
        //即将维保的
        DataStreamSource<Tuple2<Integer, Integer>> tuple2DataStreamSource4 = env.addSource(new DaPingSMZQ_WB()).setParallelism(1);
        //即将废弃的
        DataStreamSource<Tuple2<Integer, Integer>> tuple2DataStreamSource5 = env.addSource(new DaPingSMZQ_FQ()).setParallelism(1);
        //30天告警分布
        DataStreamSource<Tuple3<String, String, Integer>> tuple3DataStreamSource = env.addSource(new DaPing_ZCGJFL_30()).setParallelism(1);
        //资产大盘
        DataStreamSource<Tuple3<String, String, Integer>> tuple3DataStreamSource1 = env.addSource(new DaPingZCDP()).setParallelism(1);
        env.execute("SC sart");
    }
}
