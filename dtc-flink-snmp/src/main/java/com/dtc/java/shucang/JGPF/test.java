package com.dtc.java.shucang.JGPF;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Map;


/**
 * Created on 2019-12-30
 *
 * @author :hao.li
 */
public class test {

    public static void main(String[] args) throws Exception {
        final long windowSize = 6000L;
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        //机房总设备数
        DataStreamSource<Order> streamZS = env.addSource(new ReadDataFM()).setParallelism(1);//数据流定时从数据库中查出来数据
        //机房正常设备数
        DataStreamSource<Order> streamZC = env.addSource(new ReadDataFMZC()).setParallelism(1);//数据流定时从数据库中查出来数据
        //机房健康评分
        DataStream<Order> orderDataStream = runWindowJoin(streamZS, streamZC, windowSize);
        //未关闭告警
        DataStreamSource<Map<Integer, String>> alarmDataStream = env.addSource(new RDWGGJ()).setParallelism(1);//数据流定时从数据库中查出来数据
        orderDataStream.print();

        env.execute("zhisheng broadcast demo");
    }

    /**
 * 机房健康度
 * */
private static DataStream<Order> runWindowJoin(
        DataStreamSource<Order> grades,
        DataStreamSource<Order> salaries,
        long windowSize) {

    return grades.join(salaries)
            .where(new NameKeySelector())
            .equalTo(new NameKeySelector())

            .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))

            .apply(new JoinFunction<Order, Order, Order>() {

                @Override
                public Order join(
                        Order first,
                        Order second) {
                    double v = Double.parseDouble(String.valueOf(second.num));
                    double v1 = Double.parseDouble(String.valueOf(first.num));
                    double result = v/v1;
                    double v2 = Double.parseDouble(String.format("%.3f", result));
                    return new Order(first.id,v2);
                }
            });
}

private static class NameKeySelector implements KeySelector<Order, String> {
    @Override
    public String getKey(Order value) {
        return value.getId();
    }
}
}
