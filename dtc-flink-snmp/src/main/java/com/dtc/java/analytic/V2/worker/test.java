package com.dtc.java.analytic.V2.worker;

import com.alibaba.fastjson.JSONObject;
import com.dtc.java.analytic.V2.common.model.AlterStruct;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.common.model.SourceEvent;
import com.dtc.java.analytic.V2.common.utils.ExecutionEnvUtil;
import com.dtc.java.analytic.V2.common.utils.KafkaConfigUtil;
import com.dtc.java.analytic.V2.map.function.WinMapFunction;
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

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Created on 2019-08-12
 *
 * @author :ren
 */
@Slf4j
public class test {
    public static void main(String[] args) throws Exception {

        Map<String, String> map = new HashMap<>();
        map.put("10.3.7.231_1", "a");
        map.put("10.3.7.231_2", "b");
        map.put("10.3.7.231_3", "c");
        map.put("10.3.7.232_1", "a");
        map.put("10.3.7.232_2", "b");
        map.put("10.3.7.232_3", "c");
        String str = "123";
        double v = Double.parseDouble(str);
        System.out.println(v);

        String sjiachun = "12333";
        BigDecimal db = new BigDecimal(sjiachun);
        String ii = db.toPlainString();
        System.out.println(ii);
    }


}
