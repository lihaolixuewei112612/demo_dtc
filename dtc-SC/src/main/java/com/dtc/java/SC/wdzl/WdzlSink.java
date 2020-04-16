package com.dtc.java.SC.wdzl;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

/*我的总览 sink
* */
public class WdzlSink extends RichSinkFunction<Map<String, List>> {
    private static final String MYSQL_URL = "jdbc:mysql://10.3.7.231:3306/dtc_platform";//参数参考MySql连接数据库常用参数及代码示例
    private static final String MYSQL_NAME = "root";//数据库用户名
    private static final String MYSQL_PSD = "DTCserver2019!";//数据库密码
    private static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";//连接MySql数据库
    private Connection connection;

    private PreparedStatement preparedStatement;
    private PreparedStatement preparedStatement2;
    private PreparedStatement preparedStatement3;
    private PreparedStatement preparedStatement4;
    private PreparedStatement preparedStatement5;
    private PreparedStatement preparedStatement6;
    private PreparedStatement preparedStatement7;

    @Override

    public void open(Configuration parameters) throws Exception {

        super.open(parameters);

        // 加载JDBC驱动

        Class.forName(MYSQL_DRIVER_NAME);//加载数据库驱动

        connection = DriverManager.getConnection(MYSQL_URL, MYSQL_NAME, MYSQL_PSD);//获取连接
        //insert sql工作动态
        String sinkSql = "INSERT IGNORE INTO SC_WDZL_GZDT (`zgd`, `fpgw`, `hd`,`tw`,`zgj`,`gjdcl`,`jisuan_riqi`) VALUES (?,?,?,?,?,?,?);";
        preparedStatement = connection.prepareStatement(sinkSql);
        //insert sql 我的工单
        String sinkSql2 = "INSERT IGNORE INTO SC_WDZL_WDGD (`title`, `type`, `name`,`state`,`time`) VALUES (?,?,?,?,?);";
        preparedStatement2 = connection.prepareStatement(sinkSql2);
        //工作问题
        String sinkSql3 = "INSERT IGNORE INTO SC_WDZL_GZWT (`title`, `ans`, `name`,`time`) VALUES (?,?,?,?);";
        preparedStatement3 = connection.prepareStatement(sinkSql3);
        //知识库分布
        String sinkSql4 = "INSERT IGNORE INTO SC_WDZL_ZSKFB (`szjy`,`llzs`,`pxkc`,`jisuan_riqi`) VALUES (?,?,?,?);";
        preparedStatement4 = connection.prepareStatement(sinkSql4);
        //我处理的工单分布 sjgd,bgsx,fwgd,gjgd,zysq,wpgd,qt,js
        String sinkSql5 = "INSERT IGNORE INTO SC_WDZL_WGLDGDFB (`sjgd`, `bgsx`, `fwgd`,`gjgd`,`zysq`, `wpgd`, `qt`,`jisuan_riqi`) VALUES (?,?,?,?,?,?,?,?);";
        preparedStatement5 = connection.prepareStatement(sinkSql5);
        ////项目工单近6个个月的趋势a,b,c,d,e,f,js
        String sinkSql6 = "INSERT IGNORE INTO SC_WDZL_XMGDQS (`a`, `b`, `c`,`d`,`e`, `f`,`jisuan_riqi`) VALUES (?,?,?,?,?,?,?);";
        preparedStatement6 = connection.prepareStatement(sinkSql6);
        //资产资源分布
        String sinkSql7 = "INSERT IGNORE INTO SC_WDZL_ZCZYFB (`zhuji`,`wlsb`, `aqsb`, `ccsb`,`jcss`,`jisuan_riqi`) VALUES (?,?,?,?,?,?);";
        preparedStatement7 = connection.prepareStatement(sinkSql7);

        super.open(parameters);

    }


    @Override

    public void close() throws Exception {

        super.close();

        if (preparedStatement != null) {

            preparedStatement.close();

        }

        if (connection != null) {

            connection.close();

        }
        super.close();
    }

    @Override
    public void invoke(Map<String, List> value, Context context) throws Exception {
        //工作动态
        List listA = value.get("GZDT");
        Map mapA = null;
        for (int i = 0; i < listA.size(); i++) {
            mapA = (Map) listA.get(i);
            preparedStatement.setString(1, String.valueOf(mapA.get("zgd")));
            preparedStatement.setString(2, String.valueOf(mapA.get("fpgw")));
            preparedStatement.setString(3, String.valueOf(mapA.get("hd")));
            preparedStatement.setString(4, String.valueOf(mapA.get("tw")));
            preparedStatement.setString(5, String.valueOf(mapA.get("zgj")));
            preparedStatement.setString(6, String.valueOf(mapA.get("gjdcl")));
            preparedStatement.setString(7, String.valueOf(mapA.get("js")));
            preparedStatement.executeUpdate();
        }
        //我的工单
        List listB = value.get("WDGD");
        Map mapB = null;
        for (int i = 0; i < listB.size(); i++) {
            mapB = (Map) listB.get(i);
            preparedStatement2.setString(1, String.valueOf(mapB.get("title")));
            preparedStatement2.setString(2, String.valueOf(mapB.get("type")));
            preparedStatement2.setString(3, String.valueOf(mapB.get("name")));
            preparedStatement2.setString(4, String.valueOf(mapB.get("state")));
            preparedStatement2.setString(5, String.valueOf(mapB.get("time")));
            preparedStatement2.executeUpdate();
        }
        //工作问题
        List listC = value.get("GZWT");
        Map mapC = null;
        for (int i = 0; i < listC.size(); i++) {
            mapC = (Map) listC.get(i);
            preparedStatement3.setString(1, String.valueOf(mapC.get("title")));
            preparedStatement3.setString(2, String.valueOf(mapC.get("ans")));
            preparedStatement3.setString(3, String.valueOf(mapC.get("name")));
            preparedStatement3.setString(4, String.valueOf(mapC.get("time")));
            preparedStatement3.executeUpdate();
        }

        //知识库分布
        List listD = value.get("ZSKFB");
        Map mapD = null;
        for (int i = 0; i < listD.size(); i++) {
            mapD = (Map) listD.get(i);
            preparedStatement4.setString(1, String.valueOf(mapD.get("szjy")));
            preparedStatement4.setString(2, String.valueOf(mapD.get("llzs")));
            preparedStatement4.setString(3, String.valueOf(mapD.get("pxkc")));
            preparedStatement4.setString(4, String.valueOf(mapD.get("js")));
            preparedStatement4.executeUpdate();
        }
        //我处理的工单分布 sjgd,bgsx,fwgd,gjgd,zysq,wpgd,qt,js
        List listE = value.get("WCLDGDFB");
        Map mapE = null;
        for (int i = 0; i < listE.size(); i++) {
            mapE = (Map) listE.get(i);
            preparedStatement5.setString(1, String.valueOf(mapE.get("sjgd")));
            preparedStatement5.setString(2, String.valueOf(mapE.get("bgsx")));
            preparedStatement5.setString(3, String.valueOf(mapE.get("fwgd")));
            preparedStatement5.setString(4, String.valueOf(mapE.get("gjgd")));
            preparedStatement5.setString(5, String.valueOf(mapE.get("zysq")));
            preparedStatement5.setString(6, String.valueOf(mapE.get("wpgd")));
            preparedStatement5.setString(7, String.valueOf(mapE.get("qt")));
            preparedStatement5.setString(8, String.valueOf(mapE.get("js")));
            preparedStatement5.executeUpdate();
        }
        //项目工单近6个个月的趋势a,b,c,d,e,f,js
        List listF = value.get("WCLDGJ");
        Map mapF = null;
        for (int i = 0; i < listF.size(); i++) {
            mapF = (Map) listF.get(i);
            preparedStatement6.setString(1, String.valueOf(mapF.get("a")));
            preparedStatement6.setString(2, String.valueOf(mapF.get("b")));
            preparedStatement6.setString(3, String.valueOf(mapF.get("c")));
            preparedStatement6.setString(4, String.valueOf(mapF.get("d")));
            preparedStatement6.setString(5, String.valueOf(mapF.get("e")));
            preparedStatement6.setString(6, String.valueOf(mapF.get("f")));
            preparedStatement6.setString(7, String.valueOf(mapF.get("js")));
            preparedStatement6.executeUpdate();
        }
        //资产资源分布
        List listG = value.get("ZCZYFB");
        Map mapG = null;
        for (int i = 0; i < listG.size(); i++) {
            mapG = (Map) listG.get(i);
            preparedStatement7.setString(1, String.valueOf(mapG.get("zhuji")));
            preparedStatement7.setString(2, String.valueOf(mapG.get("wlsb")));
            preparedStatement7.setString(3, String.valueOf(mapG.get("aqsb")));
            preparedStatement7.setString(4, String.valueOf(mapG.get("ccsb")));
            preparedStatement7.setString(5, String.valueOf(mapG.get("jcss")));
            preparedStatement7.setString(6, String.valueOf(mapG.get("js")));
            preparedStatement7.executeUpdate();


        }
    }
}
