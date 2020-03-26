package com.dtc.java.shucang.JFSBWGBGJ.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @Author : lihao
 * Created on : 2020-03-26
 * @Description : TODO描述类作用
 */
@Data
@NoArgsConstructor
public class YCShu extends ZongShu{
    private double bzc;

    public YCShu(String room, String position, String box, double num, double bzc) {
        super(room, position, box, num);
        this.bzc = bzc;
    }
}
