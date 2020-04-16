package com.dtc.java.SC.JaShiCang.gldp;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;

/*驾驶舱管理大盘 sink*/
public class Lwrite  extends RichSinkFunction<Map<String, String>> {
    private static final String MYSQL_URL = "jdbc:mysql://10.3.7.231:3306/dtc_platform";//参数参考MySql连接数据库常用参数及代码示例
    private static final String MYSQL_NAME = "root";//数据库用户名
    private static final String MYSQL_PSD = "DTCserver2019!";//数据库密码
    private static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";//连接MySql数据库
    private Connection connection;

    private PreparedStatement preparedStatement;
    private PreparedStatement preparedStatement2;
    private PreparedStatement preparedStatement3;
    @Override

    public void open(Configuration parameters) throws Exception {

        super.open(parameters);

        // 加载JDBC驱动

        Class.forName(MYSQL_DRIVER_NAME);//加载数据库驱动

        connection = DriverManager.getConnection(MYSQL_URL, MYSQL_NAME, MYSQL_PSD);//获取连接
        String sinkSql = "insert ignore into jsc_gldp_gdxx (`gname`, `wcgd`, `gjsl`, `jisuan_riqi`) VALUES " +
                "(?,?,?,?);";
        preparedStatement = connection.prepareStatement(sinkSql);//insert sql在配置文件中
        String sinkSql2 = "insert ignore into jsc_gldp_bp(`wclgd` ,\n" +
                "    `jrpd` ,\n" +
                "    `jrdk` ,\n" +
                "    `zjbcs` ,\n" +
                "    `zjbrs` ,\n" +
                "    `zrs` ,\n" +
                "    `jrjbcs` ,\n" +
                "    `jrwjb` ,\n" +
                "    `yjbcs` ,\n" +
                "    `gjgd` ,\n" +
                "    `tbgjgd` ,\n" +
                "    `swgd` ,\n" +
                "    `tbswgd` ,\n" +
                "    `jisuan_riqi` ) VALUES " +
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        preparedStatement2 = connection.prepareStatement(sinkSql2);//insert sql在配置文件中
        String sinkSql3 = "insert ignore into jsc_gldp_zzfw (`sjgd`, `bgsx`,`fwgd`,`gjgd`,`zysq`,`wpgd`,`qt`,`jisuan_riqi`) VALUES " +
                "(?,?,?,?,?,?,?,?);";
        preparedStatement3 = connection.prepareStatement(sinkSql3);//insert sql在配置文件中
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
    public void invoke(Map<String, String> value, Context context) throws Exception {
        preparedStatement.setString(1, value.get("gname"));
        preparedStatement.setString(2, value.get("wcgd"));
        preparedStatement.setString(3, value.get("gjsl"));
        preparedStatement.setString(4, value.get("js"));
        preparedStatement.executeUpdate();
        preparedStatement2.setString(1, value.get("wclgd"));
        preparedStatement2.setString(2, value.get("jrpd"));
        preparedStatement2.setString(3, value.get("jrdk"));
        preparedStatement2.setString(4, value.get("zjbcs"));
        preparedStatement2.setString(5, value.get("zjbrs"));
        preparedStatement2.setString(6, value.get("zrs"));
        preparedStatement2.setString(7, value.get("jrjbcs"));
        preparedStatement2.setString(8, value.get("jrwjb"));
        preparedStatement2.setString(9, value.get("yjbcs"));
        preparedStatement2.setString(10, value.get("gjgd"));
        preparedStatement2.setString(11, value.get("tbgjgd"));
        preparedStatement2.setString(12, value.get("swgd"));
        preparedStatement2.setString(13, value.get("tbswgd"));
        preparedStatement2.setString(14, value.get("js"));
        preparedStatement2.executeUpdate();
        preparedStatement3.setString(1, value.get("sjgd"));
        preparedStatement3.setString(2, value.get("bgsx"));
        preparedStatement3.setString(3, value.get("fwgd"));
        preparedStatement3.setString(4, value.get("gjgd"));
        preparedStatement3.setString(5, value.get("zysq"));
        preparedStatement3.setString(6, value.get("wpgd"));
        preparedStatement3.setString(7, value.get("qt"));
        preparedStatement3.setString(8, value.get("js"));
        preparedStatement3.executeUpdate();

    }

}
