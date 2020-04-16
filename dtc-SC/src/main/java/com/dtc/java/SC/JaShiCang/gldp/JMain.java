package com.dtc.java.SC.JaShiCang.gldp;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Timer;
import java.util.TimerTask;

public class JMain {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                System.out.println("timer线程执行中......");
                try {
                    env.addSource(new Lreand()).addSink(new Lwrite());
                    env.execute("cai");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(task, 1000, 5000);


    }
}
