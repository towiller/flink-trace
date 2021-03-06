package org.myorg.log.etl;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;
import org.myorg.TraceEventMonitor;

import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RabbitmqLogETL {

    /**
     * @var MysqlConnectionPool
     */
    public static DruidDataSource mysqlConnectionPool;

    /**
     * @var ConnectionFactory
     */
    public static ConnectionFactory rabbitConn;

    /**
     * @param args
     */
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        mysqlConnectionPool = new DruidDataSource(); //new MysqlConnectionPool("jdbc:mysql://dev.mysql.tsingglobal.cn/metadata", "root", "Tsing1qaz@WSX", 3);
        mysqlConnectionPool.setUrl("jdbc:mysql://127.0.0.1/metadata");
        mysqlConnectionPool.setUsername("root");
        mysqlConnectionPool.setPassword("123456");
        mysqlConnectionPool.setInitialSize(2);
        mysqlConnectionPool.setMaxActive(20);
        mysqlConnectionPool.setMinIdle(2);

        //rabbitmq配置
        RMQConnectionConfig rmqConnectionConfig = new RMQConnectionConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(5672)
                .setUserName("guest")
                .setPassword("guest")
                .setVirtualHost("/")
                .build();

        DataStreamSource<String> dss = env.addSource(new RMQSource<>(
                rmqConnectionConfig, "log_collect_unstructured", true, new SimpleStringSchema()
        ));
        dss.print();


        DataStream<LogIdentifyStrEntity> ds= dss.map(new MapFunction<String, LogIdentifyStrEntity>() {

            @Override
            public LogIdentifyStrEntity map(String log) throws Exception {
                String[] spLog = log.split(" ");
                LogIdentifyStrEntity logIdentifyStrEntity = new LogIdentifyStrEntity();
                logIdentifyStrEntity.logIdentify = spLog[0];
                logIdentifyStrEntity.logStr = log;

                Statement statement = mysqlConnectionPool.getConnection().createStatement();
                String sql = "SELECT * FROM `match_log_config` WHERE `log_identify` = '" + logIdentifyStrEntity.logIdentify + "'";
                System.out.println(sql);
                ResultSet rs = statement.executeQuery(sql);

                MatchLogConfigEntity matchLogConfigEntity = null;

                while (rs.next()) {

                    matchLogConfigEntity = new MatchLogConfigEntity();

                    matchLogConfigEntity.id = rs.getInt("id");
                    matchLogConfigEntity.businessId = rs.getInt("business_id");
                    matchLogConfigEntity.regFieldLeft = rs.getString("reg_field_left");
                    matchLogConfigEntity.regFieldRight = rs.getString("reg_field_right");
                    matchLogConfigEntity.matchReg = rs.getString("match_reg");
                    matchLogConfigEntity.logIdentify = rs.getString("log_identify");
                    matchLogConfigEntity.mapDicts = rs.getString("map_dicts");
                    matchLogConfigEntity.mapDictIds = rs.getString("map_dict_ids");
                    matchLogConfigEntity.extractDict = rs.getString("extract_dicts");
                    matchLogConfigEntity.extractDictIds = rs.getString("extract_dict_ids");

                    System.out.println(rs.getInt("id") + rs.getInt("business_id"));
                    break;
                }

                logIdentifyStrEntity.matchLogConfigEntity = matchLogConfigEntity;

                return logIdentifyStrEntity;
            }
        }).filter(new FilterFunction<LogIdentifyStrEntity>() {

            @Override
            public boolean filter(LogIdentifyStrEntity entity) throws Exception {
                Boolean filterRet = false;

                if (entity.matchLogConfigEntity != null) {
                    filterRet = true;
                }

                System.out.println(filterRet);

                return filterRet;
            }
        }).process(new LogETLProcessFunction());

        try {
            env.execute("rabbitmq log etl");
        } catch (Exception e) {
            mysqlConnectionPool.close();
            e.printStackTrace();
        }
        mysqlConnectionPool.close();
    }

    public static class LogETLProcessFunction extends ProcessFunction<LogIdentifyStrEntity, LogIdentifyStrEntity> {

        /**
         * 需要提取的字段
         */
        public static ArrayList<String> fieldList = new ArrayList<String>();

        /**
         * 值列表
         */
        public static ArrayList<String> logValList = new ArrayList<String>();

        /**
         * 映射字段
         */
        public static Map<String, String> mapFiledList = new HashedMap() {};

        @Override
        public void open(Configuration conf) throws Exception {
            //super(conf);
        }

        @Override
        public void processElement(LogIdentifyStrEntity element, Context ctx, Collector<LogIdentifyStrEntity> out) throws Exception {
            MatchLogConfigEntity matchLogConfigEntity = element.matchLogConfigEntity;
            matchLogConfigEntity.matchReg = "^##log_identify## ##ip## - - \\[##create_timestamp##\\] ##logstr##$";
            System.out.println(element.logStr);

            //取出正则表达式中的所有字段
            Pattern patternField = Pattern.compile(matchLogConfigEntity.regFieldLeft + "([\\w_]+?)" + matchLogConfigEntity.regFieldRight);
            //Matcher matcherField = patternField.matcher(matchLogConfigEntity.matchReg);
            Matcher matcherField = patternField.matcher(matchLogConfigEntity.matchReg);

            System.out.println(patternField + ":" + matchLogConfigEntity.matchReg + ",group_count=" + matcherField.groupCount());
            while (matcherField.find()) {
                fieldList.add(matcherField.group());
                System.out.println("match_field:" + matcherField.group());
            }

            //替换正则表达式
            Pattern p = Pattern.compile(matchLogConfigEntity.regFieldLeft + "([\\w_]+?)" + matchLogConfigEntity.regFieldRight);
            Matcher m = p.matcher(matchLogConfigEntity.matchReg);
            String matchReg = m.replaceAll("(.+?)");
            System.out.println("match_log_reg:" + matchReg);

            //正则匹配日志数据，日志数据解析
            Pattern logPattern = Pattern.compile(matchReg);
            Matcher logMatcher = logPattern.matcher(element.logStr);
            System.out.println("group_count:" + logMatcher.groupCount());
            if (logMatcher.find()) {
                for (int l = 1; l <= logMatcher.groupCount(); l++) {
                    System.out.println(logMatcher.group(l));
                    String matchVal = logMatcher.group(l);
                    String field = fieldList.get(l-1);
                    logValList.add(logMatcher.group(l));
                    mapFiledList.put(field, matchVal);
                }
            }
            System.out.println("MatchLog: logPattern=" + logPattern + ",logValList=" + logValList + ",mapFields=" + mapFiledList);

            //字段与字典关系映射，数组转化为相应格式
            MatchLogEntity matchLogEntity = new MatchLogEntity();
            Gson gson = new Gson();
            Map<String, DictDataEntity> dictDataEntity = new HashMap<String, DictDataEntity>();
            dictDataEntity = gson.fromJson(matchLogConfigEntity.mapDicts, new TypeToken<Map<String, DictDataEntity>>(){}.getType());
            Map<String, DictDataEntity> logDictList = new HashMap<String, DictDataEntity>();
            for (String matchField : dictDataEntity.keySet()) {
                String matchVal = mapFiledList.getOrDefault(matchField, "");
                System.out.println("matchField="+ matchField + ",dictdata:" + dictDataEntity.get(matchField).dictIdentify);

                DictDataEntity logDict = dictDataEntity.get(matchField);
                logDict.val = matchVal;
                System.out.println("log_dict:" + logDict.val.toString() + ",match_val:" + matchVal +  "log_field_val_format:" + logDict.fieldValFormat);

                switch (logDict.fieldValType) {
                    case "date" :
                        SimpleDateFormat ft = new SimpleDateFormat(logDict.fieldValFormat);
                        Date t = ft.parse(matchVal);
                        if (logDict.valType == "string") {
                            logDict.val = String.format("%tF", t) + " " + String.format("%tr", t);
                        } else if (logDict.valType == "uint64" || logDict.valType == "int64") {
                            logDict.val = String.valueOf(t.getTime());
                        }
                        break;
                    default:
                        break;
                }

                logDictList.put(logDict.dictIdentify, logDict);
                System.out.println("mapfield:" + matchField + ", logdict=" + logDict);
            }
            matchLogEntity.dictDataMap = logDictList;

            matchLogEntity.createTimestamp = dictDataEntity.get("log_create_timestamp").val;
            matchLogEntity.traceId = dictDataEntity.get("log_trace_id").val;
            matchLogEntity.logIdentify = dictDataEntity.get("log_identify").val;
            matchLogEntity.dictDataMap = logDictList;
            matchLogEntity.logStr = element.logStr;

            Gson gson1 = new Gson();
            String outLog = gson1.toJson(matchLogEntity);
            System.out.println(outLog);

            element.matchLogEntity = matchLogEntity;
            System.out.println("list_dict_data:" + element);
            out.collect(element);
        }
    }

    /**
     * 日志提取
     */
    public static class LogIdentifyStrEntity {

        public String logIdentify;

        public String logStr;

        public MatchLogConfigEntity matchLogConfigEntity;

        public MatchLogEntity matchLogEntity;

        @Override
        public String toString() {
            return super.toString();
        }
    }

    public class DictDataEntity {

        @SerializedName("dict_id")
        public int dictId;

        @SerializedName("dict_type")
        public int dictType;

        @SerializedName("dict_name")
        public String dictName;

        @SerializedName("dict_enname")
        public String dictEnname;

        @SerializedName("identify")
        public String dictIdentify;

        @SerializedName("val")
        public String val;

        @SerializedName("val_type")
        public String valType;

        @SerializedName("locked")
        public int locked;

        @SerializedName("field_val_type")
        public String fieldValType;

        @SerializedName("field_val_format")
        public String fieldValFormat;
    }

    /**
     * 匹配日志实体
     */
    public static class MatchLogEntity {

        @SerializedName("trace_id")
        public String traceId;

        @SerializedName("create_timestamp")
        public String createTimestamp;

        @SerializedName("log_identify")
        public String logIdentify;

        @SerializedName("process_time")
        public Long processTime;

        @SerializedName("create_unixtime")
        public Long createUnixtime;

        @SerializedName("dict_data_map")
        public Map<String, DictDataEntity> dictDataMap;

        @SerializedName("log_str")
        public String logStr;
    }

    /**
     * 匹配日志配置实体
     */
    public static class MatchLogConfigEntity {

        public long    id;

        public long    businessId;

        public String  logIdentify;

        public long    dataType;

        public String  regFieldLeft;

        public String  regFieldRight;

        public String  matchReg;

        public String  mapDicts;

        public Map<String, DictDataEntity> mapDictEntity;

        public String  mapDictIds;

        public String extractDict;

        public String extractDictIds;
    }
}
