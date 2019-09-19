package com.jungle.bigdata.integration.kafka;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 接受Kafka的数据进行处理的BOLT
 */
public class LogProcessBolt extends BaseRichBolt {

    private OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        try {
            //这里必须填“bytes”
            byte[] binaryByField = input.getBinaryByField("bytes");

            String value = new String(binaryByField);

            /**
             * 18523981114	109.887206,31.236759	[2019-09-19 10:10:13]...........
             * 解析出来日志信息
             */

            String[] splits = value.split("\t");
            String phone = splits[0];
            String[] temp = splits[1].split(",");
            String longitude = temp[0];
            String latitude = temp[1];
            long time = DateUtils.getInstance().getTime(splits[2]);



            System.out.println(phone + "," + longitude + "," + latitude + "," + time);

            //数据类型要与数据库一一对应
            collector.emit(new Values(time, Double.parseDouble(longitude), Double.parseDouble(latitude)));
            this.collector.ack(input);
        } catch (Exception e) {
            this.collector.fail(input);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "longitude", "latitude"));
    }
}
