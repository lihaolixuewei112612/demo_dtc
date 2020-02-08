package com.dtc.java.analytic.alter;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;


/**
 * Created on 2019-12-30
 *
 * @author :hao.li
 */
@Slf4j
public class Main {

    final static MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
            "alarm_rules",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);

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

        DataStreamSource<Map<String, String>> alarmDataStream = env.addSource(new GetAlarmNotifyData()).setParallelism(1);//数据流定时从数据库中查出来数据
        alarmDataStream.print();

        SingleOutputStreamOperator<Tuple4<String, String, String, String>> map = env.addSource(new MySourceEvent()).map(new MyMapFunctionV2());

        SingleOutputStreamOperator<Tuple5<String, String, String, String, String>> alert_rule = map.connect(alarmDataStream.broadcast(ALARM_RULES))
                .process(new BroadcastProcessFunction<Tuple4<String, String, String, String>, Map<String, String>, Tuple5<String, String, String, String, String>>() {
                    MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                            "alarm_rules",
                            BasicTypeInfo.STRING_TYPE_INFO,
                            BasicTypeInfo.STRING_TYPE_INFO);

                    @Override
                    public void processElement(Tuple4<String, String, String, String> value, ReadOnlyContext ctx, Collector<Tuple5<String, String, String, String, String>> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                        String alter = value.f1;
                        if (!broadcastState.contains(alter)) {
                            return;
                        }
                        String targetId = broadcastState.get(alter);
                        if (Double.parseDouble(value.f3) > Double.parseDouble(targetId)) {
                            Tuple5<String, String, String, String, String> alter_message = Tuple5.of(value.f0, value.f1, value.f2, value.f3, targetId);
                            out.collect(alter_message);
                        }
                    }

                    @Override
                    public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<Tuple5<String, String, String, String, String>> out) throws Exception {
                        if (value != null) {
                            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                            for (Map.Entry<String, String> entry : value.entrySet()) {
                                broadcastState.put(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                });
        alert_rule.addSink(new MysqlSink(properties));
//
        alert_rule.print("alater message.....");


//        SingleOutputStreamOperator<MetricEvent> filter = alert.filter(e -> {
//            String desc = e.getTags().get("desc");
//            log.info("10**************"+desc);
//            if (desc.equals("usb插入")) {
//                return false;
//            }else {
//                return true;
//            }
//        });
//        filter.map(e->e.getName()).print("finally");
//
//        //其他的业务逻辑
//        //alert.
//
//        //然后在下游的算子中有使用到 alarmNotifyMap 中的配置信息


        env.execute("zhisheng broadcast demo");
    }

    @Slf4j
    static class MyMapFunctionV2 implements MapFunction<String, Tuple4<String, String, String, String>> {
        //对json数据进行解析并且存入Tuple
        @Override
        public Tuple4<String, String, String, String> map(String s) {
            if (s.isEmpty()) {
                //判断数据是否是空
                return Tuple4.of("null", "null", "null", "null");
            }
            if (!isJSON2(s)) {
                //判断数据是否是json格式
                return Tuple4.of("null", "null", "null", "null");
            }
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode json = null;
            try {
                json = objectMapper.readTree(s);
            } catch (IOException e) {
                log.error("Data resolve make mistake,and the reason is " + e);
                return Tuple4.of("null", "null", "null", "null");
            }
            String codes = json.get("code").textValue();
            String time = json.get("time").textValue();
            String value = json.get("value").textValue();
            String host = json.get("host").textValue().trim();
            return Tuple4.of(codes, host, time, value);
        }

        boolean isJSON2(String str) {
            boolean result = false;
            try {
                Object obj = JSON.parse(str);
                result = true;
            } catch (Exception e) {
                log.warn("Event data is not musi");
                result = false;
            }
            return result;
        }
    }
}
