package com.dtc.java.SC.wdzl;

import com.dtc.java.SC.JFSBWGBGJ.ExecutionEnvUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

public class JMainV3 {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.getConfig().setGlobalJobParameters(parameterTool);
        DataStreamSource<Map<String, List>> mapDataStreamSource = env.addSource(new WdzlSourceV3());
        mapDataStreamSource.print();
        mapDataStreamSource.addSink(new WdzlSinkV3());
        env.execute("cai");

    }
}
