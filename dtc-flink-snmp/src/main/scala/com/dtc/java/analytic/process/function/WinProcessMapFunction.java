package com.dtc.java.analytic.process.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created on 2020-01-19
 *
 * @author :hao.li
 */
@Slf4j
public class WinProcessMapFunction extends ProcessWindowFunction<Tuple6<String, String, String, String, String, String>, Tuple6<String, String, String, String, String, String>, Tuple, TimeWindow> {
    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple6<String, String, String, String, String, String>> iterable, Collector<Tuple6<String, String, String, String, String, String>> collector) throws Exception {
        int count = 0;
        double sum = 0;
        String System_name = null;
        String Host = null;
        String ZB_Name = null;
        String ZB_Code = null;
        String time = null;
        for (Tuple6<String, String, String, String, String, String> wc : iterable) {
            System_name = wc.f0;
            Host = wc.f1;
            ZB_Name = wc.f2;
            ZB_Code = wc.f3;
            time = wc.f4;
            count++;
            double value = Double.parseDouble(wc.f5);
            sum += value;
        }
        double result = sum / count;
        collector.collect(new Tuple6<>(System_name, Host, ZB_Name, ZB_Code, time, String.valueOf(result)));
    }
}
