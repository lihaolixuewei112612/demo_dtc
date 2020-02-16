package com.dtc.java.analytic.V2.worker;

import com.dtc.java.analytic.V2.common.model.AlterStruct;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.common.model.SourceEvent;
import com.dtc.java.analytic.V2.common.utils.ExecutionEnvUtil;
import com.dtc.java.analytic.V2.map.function.LinuxMapFunction;
import com.dtc.java.analytic.V2.process.function.LinuxProcessMapFunction;
import com.dtc.java.analytic.V2.sink.mysql.MysqlSink;
import com.dtc.java.analytic.V2.source.mysql.GetAlarmNotifyData;
import com.dtc.java.analytic.V2.source.test.TestSourceEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * Created on 2019-08-12
 *
 * @author :ren
 */
@Slf4j
public class StreamToFlinkV3 {
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
        int windowSizeMillis = parameterTool.getInt("dtc.windowSizeMillis", 2000);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<Map<String, String>> alarmDataStream = env.addSource(new GetAlarmNotifyData()).setParallelism(1);
        DataStreamSource<SourceEvent> streamSource = env.addSource(new TestSourceEvent());

        /**
         * {"time":"1581691002687","code":"101_101_107_105_105","host":"10.3.7.234","nameCN":"磁盘剩余大小","value":"217802544","nameEN":"disk_free"}
         * */
//        DataStreamSource<String> dataStreamSource = env.socketTextStream("172.20.10.2", 8080, '\n');

        SingleOutputStreamOperator<DataStruct> mapStream = streamSource.map(new MyMapFunctionV3());
//        SingleOutputStreamOperator<DataStruct> timeSingleOutputStream
//                = mapStream.assignTimestampsAndWatermarks(new DtcPeriodicAssigner());

        SplitStream<DataStruct> splitStream
                = mapStream.split((OutputSelector<DataStruct>) event -> {
            List<String> output = new ArrayList<>();
            String type = event.getSystem_name();
            if ("101_100".equals(type)) {
                output.add("Win");
            } else if ("101_101".equals(type)) {
                output.add("Linux");
            } else if ("102_101".equals(type)) {
                output.add("H3C_Switch");
            } else if ("102_102".equals(type)) {
                output.add("HW_Switch");
            } else if ("102_103".equals(type)) {
                output.add("ZX_Switch");
            } else if ("103_102".equals(type)) {
                output.add("DPI");
            }
            return output;
        });
        //windows指标数据处理
        DataStream<DataStruct> win = splitStream.select("Win");

        //linux指标数据处理
        DataStream<DataStruct> linuxProcess = splitStream
                .select("Linux").map(new LinuxMapFunction());


        SingleOutputStreamOperator<DataStruct> process1 = linuxProcess
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS)).process(new LinuxProcessMapFunction());
        SingleOutputStreamOperator<AlterStruct> alert_rule = process1.connect(alarmDataStream.broadcast(ALARM_RULES))
                .process(getLinuxFunction());
        //告警数据写入mysql
        alert_rule.addSink(new MysqlSink(properties));
        //告警数据实时发送kafka

        env.execute("Snmp-Data-Process");

    }

    private static BroadcastProcessFunction<DataStruct, Map<String, String>, AlterStruct> getLinuxFunction() {
        return new BroadcastProcessFunction<DataStruct, Map<String, String>, AlterStruct>() {
            MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                    "alarm_rules",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

            @Override
            public void processElement(DataStruct value, ReadOnlyContext ctx, Collector<AlterStruct> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                String alter = value.getHost();
                if (!broadcastState.contains(alter)) {
                    return;
                }
                //unique_id + ":" + code + ":" + alarm;
                String targetId = broadcastState.get(alter);
                String[] split = targetId.split(":");
                if(split.length!=3){
                    return;
                }
                String unique_id= split[0];
                String code= split[1];
                if(!code.equals(value.getZbFourName())){
                    return;
                }
                String valu= split[2];
                if(unique_id.isEmpty()||code.isEmpty()||valu.isEmpty()){
                    return;
                }
                String[] split1 = valu.split("\\|");
                if(split1.length!=3){
                    return;
                }
                Double num_1=Double.parseDouble(split1[0]);
                Double num_2=Double.parseDouble(split1[1]);
                Double num_3=Double.parseDouble(split1[2]);
                double data_value = Double.parseDouble(value.getValue());
                if((data_value>num_1||data_value==num_1)&&data_value<num_2){
                    String system_time = String.valueOf(System.currentTimeMillis());
                    AlterStruct alter_message = new AlterStruct(value.getSystem_name(),value.getHost(),value.getZbFourName(),value.getZbFourName(),value.getNameCN(),value.getNameEN(),value.getTime(),system_time,value.getValue(),"一级告警",unique_id,String.valueOf(num_1));
                    out.collect(alter_message);
                }else if((data_value>num_2||data_value==num_2)&&data_value<num_3){
                    String system_time = String.valueOf(System.currentTimeMillis());
                    AlterStruct alter_message = new AlterStruct(value.getSystem_name(),value.getHost(),value.getZbFourName(),value.getZbFourName(),value.getNameCN(),value.getNameEN(),value.getTime(),system_time,value.getValue(),"二级告警",unique_id,String.valueOf(num_2));
                    out.collect(alter_message);
                }else if(data_value>num_3||data_value==num_2){
                    String system_time = String.valueOf(System.currentTimeMillis());
                    AlterStruct alter_message = new AlterStruct(value.getSystem_name(),value.getHost(),value.getZbFourName(),value.getZbFourName(),value.getNameCN(),value.getNameEN(),value.getTime(),system_time,value.getValue(),"三级告警",unique_id,String.valueOf(num_3));
                    out.collect(alter_message);
                }
            }

            @Override
            public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<AlterStruct> out) throws Exception {
                if (value != null) {
                    BroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                    for (Map.Entry<String, String> entry : value.entrySet()) {
                        broadcastState.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        };
    }
}


@Slf4j
class MyMapFunctionV3 implements MapFunction<SourceEvent, DataStruct> {
    @Override
    public DataStruct map(SourceEvent sourceEvent) {
        String[] codes = sourceEvent.getCode().split("_");
        String systemName = codes[0].trim() + "_" + codes[1].trim();
        String zbFourCode = systemName + "_" + codes[2].trim() + "_" + codes[3].trim();
        String zbLastCode = codes[4].trim();
        String nameCN= sourceEvent.getName_CN();
        String nameEN= sourceEvent.getName_CN();
        String time = sourceEvent.getTime();
        String value = sourceEvent.getValue();
        String host = sourceEvent.getHost();
        return new DataStruct(systemName,host,zbFourCode,zbLastCode,nameCN,nameEN,time,value);
    }
}
