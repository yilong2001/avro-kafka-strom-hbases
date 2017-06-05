package com.demo.conf;

/**
 * Created by yilong on 2017/5/27.
 */
public class StreamConfigs {
    private static long hbaseBatchExePeriodMs = 1000;

    public StreamConfigs(){}

    public static long getHbaseBoltBatchExePeriodMs() {
        return hbaseBatchExePeriodMs;
    }

}
