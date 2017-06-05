package com.demo.conf;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yilong on 2017/5/27.
 */
public class HbaseTableConfig {
    public static final String TAB2_KEYID = "tab2";
    public static final String TAB2_HBASE_TABLE = "hbaseTAB2";

    public static final String TAB1_KEYID = "acpdr";
    public static final String ACPDR_HBASE_TABLE = "hbaseACPDR";

    private Map<String, String> keyTableMap = new HashMap<String, String>();

    public void init() {
        keyTableMap.put(TAB2_KEYID, TAB2_HBASE_TABLE);
        keyTableMap.put(TAB1_KEYID, ACPDR_HBASE_TABLE);
    }

    public Map<String, String> getKeyTableMap() {
        return keyTableMap;
    }

}
