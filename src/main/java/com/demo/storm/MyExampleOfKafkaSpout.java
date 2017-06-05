package com.demo.storm;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Created by yilong on 2017/5/27.
 */
public class MyExampleOfKafkaSpout extends BaseRichSpout {
    static Logger logger = Logger.getLogger(KafKaSpoutHander.class);
    String streamId = "debug_kafka_spout";

    Properties props = new Properties();
    KafkaConsumer<byte[], byte[]> consumer = null;
    long count = 0;
    SpoutOutputCollector spoutOutputCollector;
    private transient KafkaSpoutTuplesBuilder<String, byte[]> tuplesBuilder;

    public MyExampleOfKafkaSpout() {
        //props.put("security.protocol", "SASL_PLAINTEXT");
        //props.put("sasl.kerberos.service.name", "kafka");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        //props.put("bootstrap.servers", "bdap-nn-1:9092,bdap-nn-2:9092,bdap-mn-1:9092");
        props.put("group.id", "a1");

        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.consumer = new KafkaConsumer<byte[], byte[]>(props);
        this.consumer.subscribe(Arrays.asList("multihbase05"));
    }

    @Override
    public void nextTuple() {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(300);
        for (ConsumerRecord<byte[], byte[]> record : records) {
            count++;
            if (count % 10000 == 0) {
                logger.info("thread : "+Thread.currentThread().getName()+";"+" count : " + count);
            }
            final KafkaSpoutMessageId msgId = new KafkaSpoutMessageId(record);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(streamId, new Fields("topic", "partition", "key", "value"));
    }
}
