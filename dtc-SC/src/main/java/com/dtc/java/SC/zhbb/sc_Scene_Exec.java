package com.dtc.java.SC.zhbb;

import com.dtc.java.SC.JFSBWGBGJ.ExecutionEnvUtil;
import com.dtc.java.SC.common.PropertiesConstants;
import com.dtc.java.SC.zhbb.common.SC_Scene_ZHBB;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;

/**
 * @Author : lihao
 * Created on : 2020-04-15
 * @Description : 综合报表代码
 */
public class sc_Scene_Exec {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        Map<String, String> stringStringMap = parameterTool.toMap();
        Properties properties = new Properties();
        for (String key : stringStringMap.keySet()) {
            if (key.startsWith("mysql")) {
                properties.setProperty(key, stringStringMap.get(key));
            }
        }
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String database = parameterTool.get(PropertiesConstants.MYSQL_DATABASE);
        String host = parameterTool.get(PropertiesConstants.MYSQL_HOST);
        String password = parameterTool.get(PropertiesConstants.MYSQL_PASSWORD);
        String port = parameterTool.get(PropertiesConstants.MYSQL_PORT);
        String username = parameterTool.get(PropertiesConstants.MYSQL_USERNAME);
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        SC_Scene_ZHBB.sc_Scence_one(env,driver,url,username,password);
        SC_Scene_ZHBB.sc_Scence_Two(env,driver,url,username,password);

        env.execute("WordCount Example");

    }

}
