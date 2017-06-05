package com.demo.storm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.demo.conf.MyTopoConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;

import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.KafkaSpoutStreams;
import org.apache.storm.kafka.spout.KafkaSpoutStreamsNamedTopics;
import org.apache.storm.kafka.spout.KafkaSpoutTupleBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilder;
import org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilderNamedTopics;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Created by wangtuo on 17-5-18.
 */
public class KafKaSpoutHander {
    static Logger logger = Logger.getLogger(KafKaSpoutHander.class);

    public static BaseRichSpout newKafkaSpout(MyTopoConf myTopoConf) {
        //return new KafkaSpout(getKafkaSpoutConfig(myTopoConf, group));
        return new MyDebugOfKafkaSpout<String, byte[]>(getKafkaSpoutConfig(myTopoConf));
    }

    private static KafkaSpoutConfig<String, byte[]> getKafkaSpoutConfig(MyTopoConf myTopoConf) {
        Map<String, Object> kafkaConsumerProps = myTopoConf.getKafkaConsumerProps();

        KafkaSpoutStreams kafkaSpoutStreams = new KafkaSpoutStreamsNamedTopics.Builder(
                new Fields("topic", "partition", "key", "value"),
                myTopoConf.getHbaseBoltsStreamId(),
                myTopoConf.getTopics().toArray(new String[] {})
        ).build();

        KafkaSpoutTuplesBuilder<String, byte[]> tuplesBuilder = newTuplesBuilder(myTopoConf);
        KafkaSpoutRetryService retryService = newRetryService();
        return new KafkaSpoutConfig.Builder<String, byte[]>(kafkaConsumerProps, kafkaSpoutStreams, tuplesBuilder,
                retryService)
                .setMaxUncommittedOffsets(10000)
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST)
                .build();
    }

    private static KafkaSpoutRetryService newRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(new TimeInterval(500, TimeUnit.MICROSECONDS),
                TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }

    private static KafkaSpoutTuplesBuilder<String, byte[]> newTuplesBuilder(MyTopoConf myTopoConf) {
        return new KafkaSpoutTuplesBuilderNamedTopics.Builder<String, byte[]>(
                new KafkaSpoutTupleBuilder<String, byte[]>(myTopoConf.getTopics().toArray(new String[] {})) {
                    @Override
                    public List<Object> buildTuple(ConsumerRecord<String, byte[]> consumerRecord) {
                        return new Values(consumerRecord.topic(), consumerRecord.partition(),
                                consumerRecord.key(), consumerRecord.value());

                        //return new Values(consumerRecord.key(), consumerRecord.value());
                    }
                }).build();
    }
}
