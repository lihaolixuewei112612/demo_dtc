package com.dtc.java.shucang.JFSBWGBGJ;

import com.dtc.java.shucang.JFSBWGBGJ.model.YCShu;
import com.dtc.java.shucang.JFSBWGBGJ.model.ZongShu;
import com.dtc.java.shucang.JGPF.Order;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;


/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : 数仓监控大盘指标总类
 */
public class test {

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        int windowSizeMillis = 6000;
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        /**各机房各区域各机柜设备总数*/
        DataStreamSource<ZongShu> zsStream = env.addSource(new ReadDataZS()).setParallelism(1);
        SingleOutputStreamOperator<ZongShu> zsRoom = zsStream.keyBy("room").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        SingleOutputStreamOperator<ZongShu> zsPosition = zsStream.keyBy("room", "position").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        SingleOutputStreamOperator<ZongShu> zsBox = zsStream.keyBy("room", "position", "box").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        /**各机房各区域各机柜设备正常情况*/
        DataStreamSource<ZongShu> zcStream = env.addSource(new ReadDataFMZC()).setParallelism(1);
        SingleOutputStreamOperator<ZongShu> zcRoom = zcStream.keyBy("room").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        SingleOutputStreamOperator<ZongShu> zcPosition = zcStream.keyBy("room", "position").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        SingleOutputStreamOperator<ZongShu> zcBox = zcStream.keyBy("room", "position", "box").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");

        /**各区域各机柜设备未关闭告警数*/
        DataStreamSource<ZongShu> wgbgiStream = env.addSource(new ReadDataQY_WGBGJ()).setParallelism(1);
        wgbgiStream.keyBy("room").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num").print("room:");
        wgbgiStream.keyBy("room","position").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num").print("room,position:");
        wgbgiStream.keyBy("room","position","box").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num").print("room,position,box:");
        /**各机房各区域各机柜健康得分*/
        DataStream<ZongShu> recoderRoom = runWindowJoin(zsRoom, zcRoom, windowSizeMillis, "room");
        DataStream<ZongShu> recoderPosition = runWindowJoin(zsPosition, zcPosition, windowSizeMillis, "position");
        DataStream<ZongShu> recoderBox = runWindowJoin(zsBox, zcBox, windowSizeMillis, "box");

         /**各机房各区域各机柜设备正常及不正常数*/
        DataStreamSource<YCShu> ycsStream = env.addSource(new ReadDataZC_BZC()).setParallelism(1);
        ycsStream.keyBy("room", "position").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        ycsStream.keyBy("room", "position").timeWindow(Time.milliseconds(windowSizeMillis)).sum("bzc");
        ycsStream.keyBy("room", "position", "box").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        ycsStream.keyBy("room", "position", "box").timeWindow(Time.milliseconds(windowSizeMillis)).sum("bzc");
        /**各机房各区域异常设备类型分布情况*/
        DataStreamSource<ZongShu> ycFBStream = env.addSource(new ReadData_QU_YCSBFB()).setParallelism(1);
        ycFBStream.keyBy("room", "system").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        ycFBStream.keyBy("position", "system").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");

        /**各机房各区域告警设备类型分布情况*/
        DataStreamSource<ZongShu> wgbgjFBStream = env.addSource(new ReadDataQY_WGBGJFB()).setParallelism(1);
        wgbgiStream.keyBy("room","box").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        wgbgiStream.keyBy("position","box").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");


        env.execute("zhisheng broadcast demo");
    }

    /**
     * 机房健康度
     */
    private static DataStream<ZongShu> runWindowJoin(
            SingleOutputStreamOperator<ZongShu> grades,
            SingleOutputStreamOperator<ZongShu> salaries,
            long windowSize, String str) {
        DataStream<ZongShu> apply = null;
        if ("room".equals(str)) {
            apply = grades.join(salaries)
                    .where(new roomKeySelector())
                    .equalTo(new roomKeySelector())

                    .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))

                    .apply(new JoinFunction<ZongShu, ZongShu, ZongShu>() {

                        @Override
                        public ZongShu join(
                                ZongShu first,
                                ZongShu second) {
                            double v = Double.parseDouble(String.valueOf(second.getNum()));
                            double v1 = Double.parseDouble(String.valueOf(first.getNum()));
                            double result = v / v1;
                            double v2 = Double.parseDouble(String.format("%.3f", result));
                            return new ZongShu(first.getRoom(), first.getPosition(), first.getBox(), v2);
                        }
                    });
        } else if ("position".equals(str)) {
            apply = grades.join(salaries)
                    .where(new positionKeySelector())
                    .equalTo(new positionKeySelector())

                    .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))

                    .apply(new JoinFunction<ZongShu, ZongShu, ZongShu>() {

                        @Override
                        public ZongShu join(
                                ZongShu first,
                                ZongShu second) {
                            double v = Double.parseDouble(String.valueOf(second.getNum()));
                            double v1 = Double.parseDouble(String.valueOf(first.getNum()));
                            double result = v / v1;
                            double v2 = Double.parseDouble(String.format("%.3f", result));
                            return new ZongShu(first.getRoom(), first.getPosition(), first.getBox(), v2);
                        }
                    });
        } else if ("box".equals(str)) {
            apply = grades.join(salaries)
                    .where(new boxKeySelector())
                    .equalTo(new boxKeySelector())
                    .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                    .apply(new JoinFunction<ZongShu, ZongShu, ZongShu>() {
                        @Override
                        public ZongShu join(
                                ZongShu first,
                                ZongShu second) {
                            double v = Double.parseDouble(String.valueOf(second.getNum()));
                            double v1 = Double.parseDouble(String.valueOf(first.getNum()));
                            double result = v / v1;
                            double v2 = Double.parseDouble(String.format("%.3f", result));
                            return new ZongShu(first.getRoom(), first.getPosition(), first.getBox(), v2);
                        }
                    });
        }
        return apply;
    }

    private static class roomKeySelector implements KeySelector<ZongShu, String> {
        @Override
        public String getKey(ZongShu value) {
            return value.getRoom();
        }
    }

    private static class positionKeySelector implements KeySelector<ZongShu, String> {
        @Override
        public String getKey(ZongShu value) {
            return value.getRoom() + "_" + value.getPosition();
        }
    }

    private static class boxKeySelector implements KeySelector<ZongShu, String> {
        @Override
        public String getKey(ZongShu value) {
            return value.getRoom() + "_" + value.getPosition() + "_" + value.getBox();
        }
    }
}
