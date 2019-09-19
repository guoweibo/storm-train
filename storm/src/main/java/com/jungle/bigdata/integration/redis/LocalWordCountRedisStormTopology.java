package com.jungle.bigdata.integration.redis;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 使用Storm完成词频统计功能
 */
public class LocalWordCountRedisStormTopology {

    public static class DataSourceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        public static final String[] words = new String[]{"apple", "oranage", "banana", "pineapple", "water"};

        public void nextTuple() {

            //随机发送单词
            Random random = new Random();
            String word = words[random.nextInt(words.length)];

            this.collector.emit(new Values(word));

            System.out.println("emit: " + word);

            //休息一秒
            Utils.sleep(1000);


        }

        /**
         * 声明输出字段
         * 为输出的字段定义一个名字
         * @param declarer
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }


    /**
     * 对数据进行分割
     */
    public static class SplitBolt extends BaseRichBolt {

        //定义一个发射器，可以继续向下发射数据
        private OutputCollector collector;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 业务逻辑：
         *   line： 对line进行分割，按照逗号
         */
        public void execute(Tuple input) {
            String word = input.getStringByField("line");
            this.collector.emit(new Values(word));

        }

        /**
         * 为数据定义一个名字
         * @param declarer
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }


    /**
     * 词频汇总Bolt
     */
    public static class CountBolt extends  BaseRichBolt {

        //需要定义collector，向redis发送
        private OutputCollector collector;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

            this.collector=collector;
        }


        Map<String,Integer> map = new HashMap<String, Integer>();
        /**
         * 业务逻辑：
         * 1）获取每个单词
         * 2）对所有单词进行汇总
         * 3）输出
         */
        public void execute(Tuple input) {
            // 1）获取每个单词
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            if(count == null) {
                count = 0;
            }

            count ++;

            // 2）对所有单词进行汇总
            map.put(word, count);

            // 3）输出
            this.collector.emit(new Values(word, map.get(word)));
            }



        /**
         * 定义输出的值的名称
         * @param declarer
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

            declarer.declare(new Fields("word", "count"));
        }
    }

    /**
     * 数据发送至redis
     */
    public static class WordCountStoreMapper implements RedisStoreMapper {

        private RedisDataTypeDescription description;
        private final String hashKey = "wordCount";

        public WordCountStoreMapper() {
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.HASH, hashKey);
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            return tuple.getStringByField("word");
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            return tuple.getIntegerByField("count") + "";
        }
    }


    public static void main(String[] args) {

        // 通过TopologyBuilder根据Spout和Bolt构建Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        //设置关联关系
        builder.setBolt("SplitBolt", new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("SplitBolt");

        //发送数据至redis
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("192.168.1.18").setPort(53).build();
        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);
        builder.setBolt("RedisStoreBolt", storeBolt).shuffleGrouping("CountBolt");


        // 创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountRedisStormTopology",
                new Config(), builder.createTopology());

    }

}
