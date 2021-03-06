package org.myorg;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class TraceEventMonitor {

    /**
     * main
     *
     * @param args
     */
    public static void main(String[] args) {

        //创建流计算
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        //rabbitmq配置
        RMQConnectionConfig rmqConnectionConfig = new RMQConnectionConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(5672)
                .setUserName("guest")
                .setPassword("guest")
                .setVirtualHost("All")
                .build();

        //数据源设置
        DataStreamSource<String> ds= env.addSource(new RMQSource<>(
                rmqConnectionConfig, "log_collect_original", true, new SimpleStringSchema()
        ));

        ds.print();

        DataStream<LogEventMsgEntity> dsl = ds.map(new MapFunction<String, LogEventMsgEntity>() {

            @Override
            public LogEventMsgEntity map(String s) throws Exception {
                Gson gson = new Gson();
                LogEventMsgEntity ret = gson.fromJson(s, LogEventMsgEntity.class);

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = sdf.parse(ret.createTimestamp);
                ret.createUnixtime = date.getTime();

                for (String dictKey : ret.dictDataMap.keySet()) {
                    LogEventMsgEntity.DictData dictData = ret.dictDataMap.get(dictKey);
                    System.out.println(dictKey + ":dict_id=" + dictData.dictId + ",dict_identify=" + dictData.dictIdentify +  ",target_id=" + dictData.targetId);
                }
                return ret;
            }
        });
        dsl.print();

        try {
            env.execute("Job name");
        } catch (Exception e) {
            System.out.println("execute error:"  + e.getMessage());
        }
    }

    /** 水位线设置 **/
    public static class BoundedTraceEventExtractor extends BoundedOutOfOrdernessTimestampExtractor<LogEventMsgEntity> {
        Long currentMaxTimestamp = 0L;

        final Long maxOutOfOrderness = 10000L;

        public BoundedTraceEventExtractor(Time maxTime) {
            super(maxTime);
        }

        @Override
        public long extractTimestamp(LogEventMsgEntity element) {
            return element.createUnixtime;
        }
    }


    /**
     * 日志数据实体
     */
    public class LogEventMsgEntity {

        @SerializedName("trace_id")
        public String traceId;

        @SerializedName("log_identify")
        public String logIdentify;

        @SerializedName("process_time")
        public Long processTime;

        @SerializedName("create_timestamp")
        public String createTimestamp;

        @SerializedName("create_unixtime")
        public Long createUnixtime;

        @SerializedName("dict_data_map")
        public Map<String, DictData> dictDataMap;

        public String getTraceId() {
            return traceId;
        }

        public String getLogIdentify() {
            return logIdentify;
        }

        public class DictData {

            @SerializedName("dict_id")
            public Long dictId;

            @SerializedName("dict_identify")
            public String dictIdentify;

            @SerializedName("target_id")
            public Long targetId;

            @SerializedName("val")
            public String val;

            @SerializedName("val_type")
            public String valType;
        }
    }
}
