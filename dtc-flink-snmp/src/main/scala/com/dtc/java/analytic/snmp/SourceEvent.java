package com.dtc.java.analytic.snmp;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created on 2020-02-14
 *
 * @author :hao.li
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SourceEvent {
    private String time;
    private String code;
    private String host;
    private String name_CN;
    private String name_EN;
    private String value;
}
