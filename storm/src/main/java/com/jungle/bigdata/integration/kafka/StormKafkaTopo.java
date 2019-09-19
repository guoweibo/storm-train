package com.jungle.bigdata.integration.kafka;


import com.google.common.collect.Maps;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.KafkaConfig;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Map;
import java.util.UUID;

/**
 * Kafka整合Storm测试
 */
public class StormKafkaTopo {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        //kafka使用的zk地址
        ZkHosts hosts = new ZkHosts("192.168.1.18:2181");

        //kafka存储数据的topic名称
        String topic = "project_topic";

        //指定ZK中的一个根目录，存储的是KafkaSpout读取数据的位置信息（offset）
        String zkRoot = "/" + topic;

        String id = UUID.randomUUID().toString();

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, id);

        //设置读取偏移量的操作
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);


        String SPOUT_ID = KafkaSpout.class.getSimpleName();

        builder.setSpout(SPOUT_ID,kafkaSpout);

        String BOLD_ID = LogProcessBolt.class.getSimpleName();
        builder.setBolt(BOLD_ID,new LogProcessBolt()).shuffleGrouping(SPOUT_ID);


        //连接MySQL
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://192.168.1.18:9906/storm");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","123456");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "stat";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);

        builder.setBolt("JdbcInsertBolt",userPersistanceBolt).shuffleGrouping(BOLD_ID);
        //本地运行
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology(StormKafkaTopo.class.getSimpleName(),
                new Config(),
                builder.createTopology());


    }
}
