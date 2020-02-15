package com.dtc.java.analytic.V2;

import com.alibaba.fastjson.JSON;
import com.dtc.java.analytic.snmp.SourceEvent;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Created on 2020-02-15
 *
 * @author :hao.li
 */
public class test {
    public static void main(String[] args) {


        Random rand = new Random();
        List channel = Arrays.asList("1.2.3.4.5", "1.2.3.4.6", "1.2.3.4.7", "1.2.3.4.8");
        String s = readableDate();
        String str = channel.get(rand.nextInt(4)).toString();
        int value = rand.nextInt(9);
        int next = rand.nextInt(3);
        int result = rand.nextInt(100);
        int test2 = rand.nextInt(9);


        //{"time":"1581691002687","code":"101_101_107_105_105","host":"10.3.7.234","nameCN":"磁盘剩余大小","value":"217802544","nameEN":"disk_free"}

        String message = "{\"code\" : \"101_101_101_10" + next + "_10" + next+ "\",\"host\":\"" + str +
                "\",\"time\":" + "\"" + s + "\"" + ",\"value\":\"" + result + "\",\"nameCN\":\"磁盘剩余大小\",\"nameEN\":\"disk_free\"}";
        System.out.println(message);
        SourceEvent ds = JSON.parseObject(message, SourceEvent.class);
        System.out.println(ds.getCode());
    }
    private static String readableDate(){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long l = System.nanoTime();
        Date date = new Date(l);
        String format = simpleDateFormat.format(date);
        return format;
    }
}

