package com.dtc.java.SC.daping.sink;

import com.dtc.java.SC.common.PropertiesConstants;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * Created on 2019-09-12
 *
 * @author :hao.li
 */

public class MysqlSink_DP_ZCDP extends RichSinkFunction<Tuple3<String, String, Integer>> {
    private Properties properties;
    private Connection connection;
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private PreparedStatement preparedStatement;
    String str;

    public MysqlSink_DP_ZCDP(Properties prop) {
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

        String mysqlUrl = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = DriverManager.getConnection(mysqlUrl, userName
                , passWord);//写入mysql数据库
//        String sql = null;
        String sql = "replace into SC_DP_ZCDP(riqi,parent_name,child_name,num,js_time) values(?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        //insert sql在配置文件中
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
    public void invoke(Tuple3<String, String, Integer> value, Context context) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String riqi = sdf.format(System.currentTimeMillis());
        String js_time = sdf1.format(System.currentTimeMillis());

        try {
            preparedStatement.setString(1, riqi);
            preparedStatement.setString(2,value.f0);
            preparedStatement.setString(3,value.f1);
            preparedStatement.setInt(4,value.f2);
            preparedStatement.setString(5,js_time);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

