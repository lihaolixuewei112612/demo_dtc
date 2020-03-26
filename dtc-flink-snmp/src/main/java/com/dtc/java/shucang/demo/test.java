package com.dtc.java.shucang.demo;

import com.dtc.java.shucang.JFSBWGBGJ.*;
import com.dtc.java.shucang.JFSBWGBGJ.model.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

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
        DataStreamSource<ZongShu> zsStream = env.addSource(new ReadDataZS()).setParallelism(1);
        DataStream<ZongShu> zsRoom = zsStream.keyBy("room").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        DataStream<ZongShu> zsPosition = zsStream.keyBy("room", "position").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        DataStream<ZongShu> zsBox = zsStream.keyBy("room", "position", "box").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        /**各机房各区域各机柜设备正常情况*/
        DataStreamSource<ZongShu> zcStream = env.addSource(new ReadDataFMZC()).setParallelism(1);
        DataStream<ZongShu> zcRoom = zcStream.keyBy("room").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        DataStream<ZongShu> zcPosition = zcStream.keyBy("room", "position").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        DataStream<ZongShu> zcBox = zcStream.keyBy("room", "position", "box").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        /**各区域各机柜设备未关闭告警数*/
        DataStreamSource<ZongShu> wgbgiStream = env.addSource(new ReadDataQY_WGBGJ()).setParallelism(1);
        SingleOutputStreamOperator<ZongShu> sum = wgbgiStream.keyBy("room").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        SingleOutputStreamOperator<ZongShu> sum1 = wgbgiStream.keyBy("room", "position").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        SingleOutputStreamOperator<ZongShu> sum2 = wgbgiStream.keyBy("room", "position", "box").timeWindow(Time.milliseconds(windowSizeMillis)).sum("num");
        /**各机房各区域各机柜健康得分*/
        DataStream<ZongShu> recoderRoom = runWindowJoin(zsRoom, zcRoom, windowSizeMillis, "room");
        DataStream<ZongShu> recoderPosition = runWindowJoin(zsPosition, zcPosition, windowSizeMillis, "position");
        DataStream<ZongShu> recoderBox = runWindowJoin(zsBox, zcBox, windowSizeMillis, "box");
        //-----------------------------------------------------------------------------------------------------------------------------
        DataStream<RoomJoinModel> joinModelDataStream = RoomTestJoin(zsRoom, zcRoom, windowSizeMillis);
        DataStream<RoomJoinModel> joinModelDataStream1 = RoomTestJoin(sum, recoderRoom, windowSizeMillis);
        /**机房 总设备，正常数，未关闭告警数，得分*/
        DataStream<RoomResultModel> resultModelDataStream = RoomResultTestJoin(joinModelDataStream, joinModelDataStream1, windowSizeMillis);
        resultModelDataStream.print("room: ");
        //-----------------------------------------------------------------------------------------------------------------------------
        DataStream<PositionJoinModel> positionJoinModelDataStream = PositionTestJoin(zsPosition, zcPosition, windowSizeMillis);
        DataStream<PositionJoinModel> positionJoinModelDataStream1 = PositionTestJoin(sum1, recoderPosition, windowSizeMillis);
        DataStream<PositionResultModel> positionResultModelDataStream = PositionResultTestJoin(positionJoinModelDataStream, positionJoinModelDataStream1, windowSizeMillis);
        positionResultModelDataStream.print("position: ");
        //-----------------------------------------------------------------------------------------------------------------------------
        DataStream<BoxJoinModel> boxJoinModelDataStream = BoxTestJoin(zsBox, zcBox, windowSizeMillis);
        DataStream<BoxJoinModel> boxJoinModelDataStream1 = BoxTestJoin(sum2, recoderBox, windowSizeMillis);
        DataStream<BoxResultModel> boxResultModelDataStream = BoxResultTestJoin(boxJoinModelDataStream, boxJoinModelDataStream1, windowSizeMillis);
        boxResultModelDataStream.print("box: ");

        env.execute("SC sart");
    }

    /**
     * 机房健康度
     */
    private static DataStream<ZongShu> runWindowJoin(
            DataStream<ZongShu> grades,
            DataStream<ZongShu> salaries,
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
                            return new ZongShu(first.getRoom(), first.getPosition(), first.getBox(), v2,4);
                        }
                    });
        } else if ("position".equals(str)) {
            apply = grades.join(salaries)
                    .where(new positionKeySelector1())
                    .equalTo(new positionKeySelector1())

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
                            return new ZongShu(first.getRoom(), first.getPosition(), first.getBox(), v2,2);
                        }
                    });
        } else if ("box".equals(str)) {
            apply = grades.join(salaries)
                    .where(new boxKeySelector2())
                    .equalTo(new boxKeySelector2())
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
                            return new ZongShu(first.getRoom(), first.getPosition(), first.getBox(), v2,3);
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
    private static class positionKeySelector1 implements KeySelector<ZongShu, String> {
        @Override
        public String getKey(ZongShu value) {
            return value.getRoom()+"_"+value.getPosition();
        }
    }
    private static class boxKeySelector2 implements KeySelector<ZongShu, String> {
        @Override
        public String getKey(ZongShu value) {
            return value.getRoom()+"_"+value.getPosition()+"_"+value.getBox();
        }
    }

    private static class positionKeySelector implements KeySelector<PositionJoinModel, String> {
        @Override
        public String getKey(PositionJoinModel value) {
            return value.getRoom() + "_" + value.getPosition();
        }
    }

    private static class boxKeySelector implements KeySelector<BoxJoinModel, String> {
        @Override
        public String getKey(BoxJoinModel value) {
            return value.getRoom() + "_" + value.getPosition() + "_" + value.getBox();
        }
    }


    private static DataStream<RoomJoinModel> RoomTestJoin(
            DataStream<ZongShu> grades,
            DataStream<ZongShu> salaries,
            long windowSize) {
        DataStream<RoomJoinModel> apply =grades.join(salaries)
                    .where(new roomKeySelector())
                    .equalTo(new roomKeySelector())
                    .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                    .apply(new JoinFunction<ZongShu, ZongShu, RoomJoinModel>() {
                        @Override
                        public RoomJoinModel join(
                                ZongShu first,
                                ZongShu second) {
                            return new RoomJoinModel(first.getRoom(), first.getNum(), second.getNum());
                        }
                    });
        return apply;
    }
    private static DataStream<RoomResultModel> RoomResultTestJoin(
            DataStream<RoomJoinModel> grades,
            DataStream<RoomJoinModel> salaries,
            long windowSize) {
        DataStream<RoomResultModel> apply =  grades.join(salaries)
                    .where(new roomKeySelector1())
                    .equalTo(new roomKeySelector1())

                    .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))

                    .apply(new JoinFunction<RoomJoinModel, RoomJoinModel, RoomResultModel>() {
                        @Override
                        public RoomResultModel join(
                                RoomJoinModel first,
                                RoomJoinModel second) {
                            return new RoomResultModel(first.getRoom(), first.getAllNum(), first.getZcNum(), second.getAllNum(), second.getZcNum());
                        }
                    });
        return apply;
    }
    private static DataStream<PositionJoinModel> PositionTestJoin(
            DataStream<ZongShu> grades,
            DataStream<ZongShu> salaries,
            long windowSize) {
        DataStream<PositionJoinModel> apply =grades.join(salaries)
                .where(new roomKeySelector())
                .equalTo(new roomKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new JoinFunction<ZongShu, ZongShu, PositionJoinModel>() {
                    @Override
                    public PositionJoinModel join(
                            ZongShu first,
                            ZongShu second) {
                        return new PositionJoinModel(first.getRoom(), first.getPosition(),first.getNum(), second.getNum());
                    }
                });
        return apply;
    }
    private static DataStream<PositionResultModel> PositionResultTestJoin(
            DataStream<PositionJoinModel> grades,
            DataStream<PositionJoinModel> salaries,
            long windowSize) {
        DataStream<PositionResultModel> apply =  grades.join(salaries)
                .where(new positionKeySelector())
                .equalTo(new positionKeySelector())

                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))

                .apply(new JoinFunction<PositionJoinModel, PositionJoinModel, PositionResultModel>() {
                    @Override
                    public PositionResultModel join(
                            PositionJoinModel first,
                            PositionJoinModel second) {
                        return new PositionResultModel(first.getRoom(),first.getPosition(), first.getAllNum(), first.getZcNum(), second.getAllNum(), second.getZcNum());
                    }
                });
        return apply;
    }

    private static DataStream<BoxJoinModel> BoxTestJoin(
            DataStream<ZongShu> grades,
            DataStream<ZongShu> salaries,
            long windowSize) {
        DataStream<BoxJoinModel> apply =grades.join(salaries)
                .where(new roomKeySelector())
                .equalTo(new roomKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new JoinFunction<ZongShu, ZongShu, BoxJoinModel>() {
                    @Override
                    public BoxJoinModel join(
                            ZongShu first,
                            ZongShu second) {
                        return new BoxJoinModel(first.getRoom(), first.getPosition(),first.getBox(),first.getNum(), second.getNum());
                    }
                });
        return apply;
    }
    private static DataStream<BoxResultModel> BoxResultTestJoin(
            DataStream<BoxJoinModel> grades,
            DataStream<BoxJoinModel> salaries,
            long windowSize) {
        DataStream<BoxResultModel> apply =  grades.join(salaries)
                .where(new boxKeySelector())
                .equalTo(new boxKeySelector())

                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))

                .apply(new JoinFunction<BoxJoinModel, BoxJoinModel, BoxResultModel>() {
                    @Override
                    public BoxResultModel join(
                            BoxJoinModel first,
                            BoxJoinModel second) {
                        return new BoxResultModel(first.getRoom(),first.getPosition(), first.getBox(),first.getAllNum(), first.getZcNum(), second.getAllNum(), second.getZcNum());
                    }
                });
        return apply;
    }
    private static class roomKeySelector1 implements KeySelector<RoomJoinModel, String> {
        @Override
        public String getKey(RoomJoinModel value) {
            return value.getRoom();
        }
    }
    private static class roomKeySelector2 implements KeySelector<BoxJoinModel, String> {
        @Override
        public String getKey(BoxJoinModel value) {
            return value.getRoom()+"_"+value.getPosition();
        }
    }
    private static class roomKeySelector3 implements KeySelector<BoxJoinModel, String> {
        @Override
        public String getKey(BoxJoinModel value) {
            return value.getRoom()+"_"+value.getPosition()+"_"+value.getBox();
        }
    }
}
