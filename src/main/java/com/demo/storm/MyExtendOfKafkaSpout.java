package com.demo.storm;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.Map;


/**
 * Created by yilong on 2017/6/1.
 */
public class MyExtendOfKafkaSpout extends KafkaSpout<String, byte[]> {
    private static final Logger LOG = Logger.getLogger(MyExtendOfKafkaSpout.class);

    public MyExtendOfKafkaSpout(KafkaSpoutConfig<String, byte[]> kafkaSpoutConfig) {
        super(kafkaSpoutConfig);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
    }

    @Override
    public void nextTuple() {
        LOG.info("******************* nextTuple *******************");
        super.nextTuple();
    }

    @Override
    public void ack(Object messageId) {
        LOG.info("*********************** ack *******************");
        LOG.info(messageId.toString());
        super.ack(messageId);
    }

    @Override
    public void fail(Object messageId) {
        LOG.info("*********************** fail *******************");
        LOG.info(messageId.toString());
        super.ack(messageId);
    }

    @Override
    public void activate() {
        LOG.info("*********************** activate *******************");
        super.activate();
    }

    @Override
    public void deactivate() {
        LOG.info("*********************** deactivate *******************");
        super.deactivate();
    }

    @Override
    public void close() {
        LOG.info("*********************** activate *******************");
        super.close();
    }

    @Override
    public void setWaitingToEmit(ConsumerRecords<String, byte[]> consumerRecords) {
        LOG.info("*********************** setWaitingToEmit:"+consumerRecords.count()+" *******************");
        super.setWaitingToEmit(consumerRecords);
    }




}
