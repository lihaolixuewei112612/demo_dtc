package com.dtc.java.analytic.V2.worker;

import com.dtc.java.analytic.V2.common.model.AlterStruct;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.common.model.SourceEvent;
import com.dtc.java.analytic.V2.common.utils.ExecutionEnvUtil;
import com.dtc.java.analytic.V2.common.utils.KafkaConfigUtil;
import com.dtc.java.analytic.V2.map.function.LinuxMapFunction;
import com.dtc.java.analytic.V2.map.function.WinMapFunction;
import com.dtc.java.analytic.V2.process.function.LinuxProcessMapFunction;
import com.dtc.java.analytic.V2.process.function.WinProcessMapFunction;
import com.dtc.java.analytic.V2.sink.mysql.MysqlSink;
import com.dtc.java.analytic.V2.sink.opentsdb.PSinkToOpentsdb;
import com.dtc.java.analytic.V2.source.mysql.GetAlarmNotifyData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
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
public class StreamToFlinkV3Copy {
    final static MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
            "alarm_rules",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);
    private static DataStreamSource<Map<String, String>> alarmDataStream = null;

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
        env.getConfig().setGlobalJobParameters(parameterTool);
        alarmDataStream = env.addSource(new GetAlarmNotifyData()).setParallelism(1);
//        DataStreamSource<SourceEvent> streamSource = env.addSource(new TestSourceEvent());
        DataStreamSource<SourceEvent> streamSource = KafkaConfigUtil.buildSource(env);
        streamSource.print("last: ");

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
        //linux指标数据处理
        SingleOutputStreamOperator<DataStruct> linuxProcess = splitStream
                .select("Linux")
                .map(new LinuxMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new LinuxProcessMapFunction());
        linuxProcess.print("test:");


        //告警数据实时发送kafka
        env.execute("Snmp-Data-Process");
    }
}


@Slf4j
class MyMapFunctionV4 implements MapFunction<SourceEvent, DataStruct> {
    @Override
    public DataStruct map(SourceEvent sourceEvent) {
        String[] codes = sourceEvent.getCode().split("_");
        String systemName = codes[0].trim() + "_" + codes[1].trim();
        String zbFourCode = systemName + "_" + codes[2].trim() + "_" + codes[3].trim();
        String zbLastCode = codes[4].trim();
        String nameCN = sourceEvent.getNameCN();
        String nameEN = sourceEvent.getNameEN();
        String time = sourceEvent.getTime();
        String value = sourceEvent.getValue();
        String host = sourceEvent.getHost();
        return new DataStruct(systemName, host, zbFourCode, zbLastCode, nameCN, nameEN, time, value);
    }
}
