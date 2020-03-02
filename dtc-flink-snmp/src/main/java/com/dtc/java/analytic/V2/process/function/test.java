package com.dtc.java.analytic.V2.process.function;

import java.util.HashMap;
import java.util.Map;

/**
 * Created on 2020-03-02
 *
 * @author :hao.li
 */
public class test {
    public static void main(String[] args) {
        Map<String,String> mapA = new HashMap<>();
        Map<String,String> mapB = new HashMap<>();
        mapA.put("a","2");
        mapA.put("b","3");
        mapA.put("c","6");
        mapB.put("a","3");
        mapB.put("b","5");
        mapB.put("c","7");
        Double re=0.0;
        Double res =0.0;
        for(String s:mapA.values()){
           re += Double.parseDouble(s);
        }
        for (String s:mapB.values()){
             res +=Double.parseDouble(s);
        }
        System.out.println(re);
        System.out.println(res);
        System.out.println(re/res);

    }
}
