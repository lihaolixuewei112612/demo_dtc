package com.dtc.java.analytic.V2;
import com.dtc.java.analytic.common.constant.PropertiesConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * Created on 2019-09-12
 *
 * @author :hao.li
 */

public class MysqlSink extends RichSinkFunction<AlterStruct> {
    private Properties properties;
    private Connection connection;
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private PreparedStatement preparedStatement;
    public MysqlSink(Properties prop){
        this.properties = prop;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(JDBC_DRIVER);
        // 获取数据库连接
        String userName = properties.get(PropertiesConstants.MYSQL_USERNAME).toString();
        String passWord = properties.get(PropertiesConstants.MYSQL_PASSWORD).toString();
        String host = properties.get(PropertiesConstants.MYSQL_HOST).toString();
        String port = properties.get(PropertiesConstants.MYSQL_PORT).toString();
        String database = properties.get(PropertiesConstants.MYSQL_DATABASE).toString();

        String mysqlUrl= "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = DriverManager.getConnection(mysqlUrl,userName
               ,passWord);//写入mysql数据库
        preparedStatement = connection.prepareStatement(properties.get(PropertiesConstants.SQL).toString());//insert sql在配置文件中
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(AlterStruct value, Context context) throws Exception {
        try {

            /**
             *
             * private String System_name;
             *     private String Host;
             *     private String zbFourName;
             *     private String zbLastCode;
             *     private String nameCN;
             *     private String nameEN;
             *     private String event_time;
             *     private String system_time;
             *     private String value;
             *     private String level;
             *     private String unique_id;
             *     private String yuzhi;
             */
            String system_code = value.getSystem_name();
            String host = value.getHost();
            String code = value.getZbLastCode();//获取JdbcReader发送过来的结果
            String nameCN=value.getNameCN();
            String nameEN = value.getNameEN();
            String event_time =value.getEvent_time();
            String system_time=value.getSystem_time();
            String result = value.getValue();
            String level =value.getLevel();
            String unique_id = value.getUnique_id();
            String yuzhi =value.getYuzhi();
            preparedStatement.setString(1,system_code);
            preparedStatement.setString(2,host);
            preparedStatement.setString(3,code);
            preparedStatement.setString(4,nameCN);
            preparedStatement.setString(5,nameEN);
            preparedStatement.setString(6,event_time);
            preparedStatement.setString(7,system_time);
            preparedStatement.setString(8,result);
            preparedStatement.setString(9,level);
            preparedStatement.setString(10,unique_id);
            preparedStatement.setString(11,yuzhi);
            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

