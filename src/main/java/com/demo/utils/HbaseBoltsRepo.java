package com.demo.utils;

import com.demo.conf.HbaseTableConfig;
import com.demo.storm.SingleHbaseBolt;
import org.apache.storm.hbase.bolt.AbstractHBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import tables.tab1.TAB1;
import tables.tab2.TAB2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yilong on 2017/5/27.
 */
public class HbaseBoltsRepo {
    private Map<String, AbstractHBaseBolt> hbaseMap = new HashMap<String, AbstractHBaseBolt>();

    SingleHbaseBolt<TAB1, tables.tab1.columns> tab1HbaseBolt;
    SingleHbaseBolt<TAB2, tables.tab2.columns> tab2HbaseBolt;

    public HbaseBoltsRepo(Map<String, String> keyTableMap) {
        tab1HbaseBolt = new SingleHbaseBolt<TAB1, tables.tab1.columns>(keyTableMap.get(HbaseTableConfig.TAB1_KEYID),
                new SimpleHBaseMapper(), TAB1.class).withConfigKey("HbaseConf");

        tab2HbaseBolt = new SingleHbaseBolt<TAB2, tables.tab2.columns>(keyTableMap.get(HbaseTableConfig.TAB2_KEYID),
                new SimpleHBaseMapper(), TAB2.class).withConfigKey("HbaseConf");

        hbaseMap.put(HbaseTableConfig.TAB1_KEYID, tab1HbaseBolt);
        hbaseMap.put(HbaseTableConfig.TAB2_KEYID, tab2HbaseBolt);
    }

    public Map<String, AbstractHBaseBolt> getHbaseMap() {
        return hbaseMap;
    }

    public AbstractHBaseBolt getHbaseBoltByKeyId(String keyId) {
        if (keyId == null) {
            return null;
        }

        if (!hbaseMap.containsKey(keyId)) {
            return null;
        }

        return hbaseMap.get(keyId);
    }

    public String getIdByTupleKey(String tupleKey) {
        if (tupleKey.toLowerCase().contains(HbaseTableConfig.TAB1_KEYID)) {
            return HbaseTableConfig.TAB1_KEYID;
        } else if (tupleKey.toLowerCase().contains(HbaseTableConfig.TAB2_KEYID)) {
            return HbaseTableConfig.TAB2_KEYID;
        } else {
            return null;
        }
    }
}
