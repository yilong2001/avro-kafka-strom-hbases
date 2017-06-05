package com.demo.storm;

import com.demo.conf.HbaseTableConfig;
import com.demo.conf.MyTopoConf;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


/**
 * Created by wangtuo on 17-5-17.
 */
public class KafkaToHBaseTopology {
    static org.apache.log4j.Logger LOG = KafKaSpoutHander.logger;//org.apache.log4j.Logger.getLogger(KafkaToHBaseTopology.class);

    private static String topoName = "kafkaToHbase";

    public static void main(String[] args) {
        if (args.length > 0) {
            topoName = args[0];
        }

        HbaseTableConfig hbaseTableConfig = new HbaseTableConfig();
        hbaseTableConfig.init();

        try {
            MyTopoConf myTopoConf = MyTopoConf.newTopoConf();
            //List<KafkaConsumerGroupTopo> groups = myTopoConf.initConf();

            TopologyBuilder builder = new TopologyBuilder();

            Config conf = myTopoConf.getTopoConf();
            conf.put("HbaseConf", myTopoConf.getHbaseConf());

            //KafkaConsumerGroupTopo group = groups.get(0);

            String tops = "topics : ";
            for (String top : myTopoConf.getTopics()) {
                tops += top+",";
            }

            LOG.info(tops);

            String[] components = {"kafkaSpout-1", "hbaseBolt-all"};
            int parallelism = 4; //group.getKafkaSpoutConf().get("parallelism");

            //builder.setSpout(components[0], new MyExampleOfKafkaSpout(), parallelism);

            builder.setSpout(components[0], KafKaSpoutHander.newKafkaSpout(myTopoConf), parallelism);

            //builder.setBolt(components[1],new SplitFlowBolt(group.getHbaseBoltsStreamId()), parallelism)
            //        .fieldsGrouping(components[0], group.getHbaseBoltsStreamId(), new Fields("topic", "partition"));

            builder.setBolt(components[1], new MultiHbaseBolt(hbaseTableConfig.getKeyTableMap()), 2*parallelism).
                    fieldsGrouping(components[0], myTopoConf.getHbaseBoltsStreamId(), new Fields("key"));

            StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
        } catch (Exception e) {
            LOG.error("topology:" + topoName + "submit fiald" + e.getMessage(), e);
        }
    }
}
