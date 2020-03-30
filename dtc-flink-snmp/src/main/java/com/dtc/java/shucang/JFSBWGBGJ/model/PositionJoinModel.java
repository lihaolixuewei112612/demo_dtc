package com.dtc.java.shucang.JFSBWGBGJ.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : lihao
 * Created on : 2020-03-26
 * @Description : TODO描述类作用
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PositionJoinModel{
    private String partitions;
    private String room;
    private double allNum;
    private double zcNum;
}
