package com.demo.storm;

/**
 * Created by yilong on 2017/5/24.
 */

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.demo.conf.StreamConfigs;
import com.demo.utils.AvroSerd;
import com.demo.utils.Timer;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hbase.bolt.AbstractHBaseBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

/**
 * Created by wangtuo on 17-5-15.
 */
public class SingleHbaseBolt<T, C> extends AbstractHBaseBolt {
    private static final Logger LOG = Logger.getLogger(SingleHbaseBolt.class);

    private static final int DEFAULT_FLUSH_INTERVAL_SECS = 1;
    boolean writeToWAL = true;
    List<Mutation> batchMutations;
    int flushIntervalSecs = DEFAULT_FLUSH_INTERVAL_SECS;
    int batchSize = 10;
    BatchHelper batchHelper;
    Class<?> tableClass;
    String[] familys;
    Character op_type = 'I';
    Timer execTimer;

    public SingleHbaseBolt(String tableName, HBaseMapper mapper, Class<T> tableClass) {
        super(tableName, mapper);
        this.batchMutations = new LinkedList<Mutation>();
        this.familys = new String[]{"F"};
        this.tableClass = tableClass;
    }

    public SingleHbaseBolt writeToWAL(boolean writeToWAL) {
        this.writeToWAL = writeToWAL;
        return this;
    }

    public SingleHbaseBolt withConfigKey(String configKey) {
        this.configKey = configKey;
        return this;
    }

    public SingleHbaseBolt withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public SingleHbaseBolt withFlushIntervalSecs(int flushIntervalSecs) {
        this.flushIntervalSecs = flushIntervalSecs;
        return this;
    }

    public SingleHbaseBolt withOp_Tpye(Character op_tpye) {
        this.op_type = Arrays.asList('I', 'U', 'D', 'T').contains(op_type) ? op_type : 'I';
        return this;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), flushIntervalSecs);
    }

    //@Override
    public void execute(Tuple tuple) {
        LOG.info("****** hbasebolt : " + tuple.getSourceComponent() + " : " + tuple.getSourceStreamId() +" : " + batchHelper.getBatchSize());
        try {
            if ((tuple != null) && batchHelper.shouldHandle(tuple)) {
                byte[] rowKey = (byte[]) tuple.getValueByField("key");

                @SuppressWarnings("unchecked")
                T tab = (T) AvroSerd.deserialize((byte[]) tuple.getValueByField("value"), tableClass);

                ColumnList cols = null;
                C col = null;
                try {
                    col = (C) tableClass.getMethod("getAfter").invoke(tab);
                } catch (Exception e) {
                    LOG.error("****** hbasebolt getafter error " + e.getMessage());
                    this.collector.ack(tuple);
                    return;
                }

                try {
                    cols = getColumnList(col, Bytes.toBytes(familys[0]));
                } catch (Exception e) {
                    LOG.error("****** hbasebolt getcolumn error : " + e.getMessage());
                    this.collector.ack(tuple);
                    return;
                }

                Character op_type_data = 'I';
                try {
                    op_type_data = tableClass.getMethod("getOpType").invoke(tab).toString().toUpperCase().charAt(0);
                    op_type_data = Arrays.asList('I', 'U', 'D', 'T').contains(op_type_data) ? op_type_data : this.op_type;
                } catch (Exception e) {
                    LOG.error("****** hbasebolt op_type eror : " + e.getMessage());
                    this.collector.ack(tuple);
                    return;
                }

                switch (op_type_data) {
                    case 'I':
                    case 'U':
                        batchMutations.addAll(
                                getPutMutations(rowKey, cols, writeToWAL ? Durability.SYNC_WAL : Durability.SKIP_WAL));
                        break;
                    case 'D':
                        batchMutations.addAll(
                                getDeleteMutations(rowKey, cols, writeToWAL ? Durability.SYNC_WAL : Durability.SKIP_WAL));
                        break;
                    case 'T':
                    default:
                        LOG.warn("******** hbasebolt op_type " + op_type_data + " not support. ");
                }

                batchHelper.addBatch(tuple);
            }

            if (batchHelper.shouldFlush() || execTimer.isExpiredResetOnTrue()) {
                LOG.info("******** hbasebolt-flush : " + batchHelper.getBatchSize());
                try {
                    this.hBaseClient.batchMutate(batchMutations);
                } catch (Exception e) {
                    LOG.error("******* hbasebolt-flush-error : " + e.getMessage());
                }
                batchHelper.ack();
                batchMutations.clear();
            }
        } catch (Exception e) {
            LOG.error("******* hbasebolt-error : " + e.getMessage());
            batchHelper.fail(e);
            batchMutations.clear();
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(map, topologyContext, collector);
        LOG.info("prepare: bathSize: " +  batchSize);
        this.batchHelper = new BatchHelper(batchSize, collector);
        this.execTimer = new Timer(1000, StreamConfigs.getHbaseBoltBatchExePeriodMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private List<Mutation> getPutMutations(byte[] rowKey, ColumnList cols, Durability durability) {
        List<Mutation> mutations = Lists.newArrayList();
        if (cols.hasColumns()) {
            Put put = new Put(rowKey);
            put.setDurability(durability);
            for (ColumnList.Column col : cols.getColumns()) {
                if (col.getTs() > 0) {
                    put.addColumn(col.getFamily(), col.getQualifier(), col.getTs(), col.getValue());
                } else {
                    put.addColumn(col.getFamily(), col.getQualifier(), col.getValue());
                }
            }
            LOG.info("the put object: " + put.getRow() + ":" + put.getFamilyCellMap());
            mutations.add(put);
        }

        if (cols.hasCounters()) {
            Increment inc = new Increment(rowKey);
            inc.setDurability(durability);
            for (ColumnList.Counter cnt : cols.getCounters()) {
                inc.addColumn(cnt.getFamily(), cnt.getQualifier(), cnt.getIncrement());
            }
            mutations.add(inc);
        }

        if (mutations.isEmpty()) {
            mutations.add(new Put(rowKey));
        }
        return mutations;
    }

    private List<Mutation> getDeleteMutations(byte[] rowKey, ColumnList cols, Durability durability) {
        List<Mutation> mutations = Lists.newArrayList();
        if (cols.hasColumns()) {
            Delete delete = new Delete(rowKey);
            delete.setDurability(durability);

            LOG.info("set delete" + durability);
            for (ColumnList.Column col : cols.getColumns()) {
                if (col.getTs() > 0) {
                    delete.addColumn(col.getFamily(), col.getQualifier(), col.getTs());
                } else {
                    delete.addColumn(col.getFamily(), col.getQualifier());
                }
            }
            LOG.info("and delete" + delete.getId() + Bytes.toString(delete.getRow()) + delete.toString());
            mutations.add(delete);
        }

        if (cols.hasCounters()) {
            Increment inc = new Increment(rowKey);
            inc.setDurability(durability);
            for (ColumnList.Counter cnt : cols.getCounters()) {
                inc.addColumn(cnt.getFamily(), cnt.getQualifier(), cnt.getIncrement());
            }
            mutations.add(inc);
        }

        if (mutations.isEmpty()) {
            mutations.add(new Put(rowKey));
        }
        return mutations;
    }

    /**
     * get columns
     *
     * @param col
     * @return
     */
    @SuppressWarnings("unchecked")
    private ColumnList getColumnList(C col, byte[] family, long ts) {
        ColumnList columnList = new ColumnList();
        Class<C> COL = (Class<C>) col.getClass();
        List<Field> fields = Arrays.asList(COL.getFields());

        Set<String> fieldnameWithIsMissings = new HashSet<>();
        for (Field field : fields) {
            if (org.apache.avro.Schema.class.equals(field.getType())) {
                continue;
            }

            String fieldName = field.getName();
            if (fieldName.endsWith("_isMissing")) {
                boolean isMiss = true;
                try {
                    field.setAccessible(false);
                    isMiss = field.getBoolean(col);
                } catch (Exception e) {
                    LOG.warn(e.getMessage(), e);
                }

                if (!isMiss) {
                    fieldnameWithIsMissings.add(fieldName);
                }
            }
        }

        for (Field field : fields) {
            if (org.apache.avro.Schema.class.equals(field.getType())) {
                continue;
            }

            String fieldName = field.getName();
            if (fieldName.endsWith("_isMissing")) {
                continue;
            }

            String tmp = fieldName + "_isMissing";
            if (!fieldnameWithIsMissings.contains(tmp)) {
                continue;
            }

            field.setAccessible(false);
            Object o = null;
            try {
                o = field.get(col);
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
            }

            if (o != null) {
                columnList.addColumn(family, Bytes.toBytes(fieldName), ts, Bytes.toBytes(o.toString()));
            } else {
                LOG.warn("" + fieldName + " : addColumn failed! ");
            }
        }

        return columnList;
    }

    private ColumnList getColumnList(C col, byte[] family) {
        return this.getColumnList(col, family, -1L);
    }

}
