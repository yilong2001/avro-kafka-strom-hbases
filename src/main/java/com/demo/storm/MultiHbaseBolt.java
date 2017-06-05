package com.demo.storm;

import com.demo.conf.StreamConfigs;
import com.demo.utils.HbaseBoltsRepo;
import com.demo.utils.Timer;
import org.apache.storm.hbase.bolt.AbstractHBaseBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.log4j.Logger;

import org.apache.storm.utils.TupleUtils;
import tables.tab1.TAB1;
import tables.tab1.columns;
import tables.tab2.TAB2;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by yilong on 2017/5/24.
 */
public class MultiHbaseBolt extends BaseRichBolt {
    private static final Logger LOG = Logger.getLogger(SingleHbaseBolt.class);

    SingleHbaseBolt<TAB1, columns> acpdrHbaseBolt;
    SingleHbaseBolt<TAB2, tables.tab2.columns> tab2HbaseBolt;
    private String streamId = null;
    OutputCollector collector = null;
    Timer execTimer;

    HbaseBoltsRepo hbaseBoltsRepo;
    Map<String, String> keyTableMap;

    public MultiHbaseBolt(Map<String, String> keyTableMap) {
        this.keyTableMap = keyTableMap;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            Map<String, AbstractHBaseBolt> hBaseBoltMap = hbaseBoltsRepo.getHbaseMap();
            for (Map.Entry<String, AbstractHBaseBolt> entry : hBaseBoltMap.entrySet()) {
                entry.getValue().execute(tuple);
            }
            return;
        }

        String key = "";
        try {
            key = new String((byte[]) tuple.getValueByField("key"), "UTF-8");
        } catch (Exception e) {
            LOG.error("**************, hbasebolts-key-error : "+e.getMessage()+": key = "+key);
            this.collector.fail(tuple);
            return;
        }

        LOG.info("**************, hbasebolts-key = "+key);
        AbstractHBaseBolt hBaseBolt = hbaseBoltsRepo.getHbaseBoltByKeyId(hbaseBoltsRepo.getIdByTupleKey(key));
        if (hBaseBolt == null) {
            LOG.error("**************, hbasebolts-BaseBolt=null ******** ");
            this.collector.fail(tuple);
            return;
        } else {
            hBaseBolt.execute(tuple);
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        this.hbaseBoltsRepo = new HbaseBoltsRepo(keyTableMap);
        this.execTimer = new Timer(1000, StreamConfigs.getHbaseBoltBatchExePeriodMs(), TimeUnit.MILLISECONDS);

        Map<String, AbstractHBaseBolt> hBaseBoltMap = hbaseBoltsRepo.getHbaseMap();
        for (Map.Entry<String, AbstractHBaseBolt> entry : hBaseBoltMap.entrySet()) {
            entry.getValue().prepare(map, topologyContext, collector);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declareStream(this.streamId, new Fields("key", "value"));
    }

}
