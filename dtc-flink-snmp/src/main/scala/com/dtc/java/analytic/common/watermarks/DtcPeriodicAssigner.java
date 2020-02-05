package com.dtc.java.analytic.common.watermarks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

public class DtcPeriodicAssigner implements AssignerWithPeriodicWatermarks<Tuple6<String, String, String, String, String, String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DtcPeriodicAssigner.class);

    private long maxOutOfOrderness = 2000; // 2 seconds

    private long currentMaxTimestamp = 0L;

    long lastEmittedWatermark = Long.MIN_VALUE;


    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        Long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
        if (potentialWM >= lastEmittedWatermark) {
            lastEmittedWatermark = potentialWM;
        }
        Watermark watermark = new Watermark(potentialWM);
        return watermark;
    }

    @Override
    public long extractTimestamp(Tuple6<String, String, String, String, String, String> s, long l) {
        long timestamp = Long.parseLong(s.f4);
        return timestamp;
    }
}
