package com.dtc.java.analytic.V2.sink.mysql;
import com.dtc.java.analytic.V2.common.constant.PropertiesConstants;
import com.dtc.java.analytic.V2.common.model.AlterStruct;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;
import java.util.Random;

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
            String s = UUIDGenerator.generateUserCode();
            String system_id = value.getSystem_name();
            String host_ip = value.getHost();
            String itmes_code = value.getZbFourName();
            String last_code =value.getZbLastCode();
            String nameCN=value.getNameCN();
            String nameEN = value.getNameEN();
            String event_time =value.getEvent_time();
            String system_time=value.getSystem_time();
            String real_value = value.getValue();
            String alarm_threshold = value.getYuzhi();
            String unique_id = value.getUnique_id();
            String alarm_garde =value.getLevel();
            preparedStatement.setString(1,system_id);
            preparedStatement.setString(2,host_ip);
            preparedStatement.setString(3,itmes_code);
            preparedStatement.setString(4,last_code);
            preparedStatement.setString(5,nameCN);
            preparedStatement.setString(6,nameEN);
            preparedStatement.setString(7,event_time);
            preparedStatement.setString(8,system_time);
            preparedStatement.setString(9,real_value);
            preparedStatement.setString(10,alarm_threshold);
            preparedStatement.setString(11,unique_id);
            preparedStatement.setString(12,alarm_garde);
            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
class UUIDGenerator {

    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random rng = new SecureRandom();

    private static char randomChar(){
        return ALPHABET.charAt(rng.nextInt(ALPHABET.length()));
    }

    public static String uuid(int length, int spacing, char spacerChar){
        StringBuilder sb = new StringBuilder();
        int spacer = 0;
        while(length > 0){
            if(spacer == spacing){
                sb.append(spacerChar);
                spacer = 0;
            }
            length--;
            spacer++;
            sb.append(randomChar());
        }
        return sb.toString();
    }

    public static String generateUserCode() {
        return uuid(6, 10, ' ');
    }

//    public static void main(String[] args) {
//        System.out.println(generateUserCode());
//    }
}

