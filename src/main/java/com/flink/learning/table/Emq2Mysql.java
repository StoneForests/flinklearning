package com.flink.learning.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author shilinlin
 * @program: flinklearning
 * @description:
 * @date 2024-06-05 18:02:05
 */
public class Emq2Mysql {
    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        String sourceSql = "CREATE TABLE emqsource ( id BIGINT, nm STRING, dt timestamp, PRIMARY KEY (id) NOT ENFORCED ) WITH ( 'connector' = 'mqtt', 'hostUrl' = 'tcp://10.0.113.61:1883', 'username'= 'emqxadmin', 'password'= 'JevDDzb!5Qfh5Fmr', 'topics' = 'mqtt/source1,mqtt/source2', 'clientIdPrefix' = 'source_client_123456', 'cleanSession' = 'true', 'autoReconnect' = 'true', 'connectionTimeout' = '30', 'keepAliveInterval' = '60', 'format' = 'json', 'maxInflight'= '100', 'pollInterval'= '5000' )";

        tEnv.executeSql(sourceSql);

        String sinkSql = "CREATE TABLE mysqlSink ( id BIGINT, nm STRING, dt timestamp, PRIMARY KEY (id) NOT ENFORCED ) WITH ( 'connector' = 'jdbc', 'url' = 'jdbc:mysql://10.0.113.65:3306/autotest', 'table-name' = 'emq', 'username'= 'sybd', 'password'= 'nqG6HouR_9nwV7b', 'driver' = 'com.mysql.cj.jdbc.Driver' )" ;
        tEnv.executeSql(sinkSql);

        //将flink_mqtt_source中的结果写入到flink_mqtt_sink表中，该语句会一直从mqtt中读取数据并写入到mqtt
        tEnv.executeSql("insert into mysqlSink select id,nm,dt from emqsource");
    }
}
