package com.caihua.flume_kafka;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 1.Flume对接KafkaSink
 */
public class KafkaSink {
    /**
     * 1）配置文件-sink：
     * a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
     * a1.sinks.k1.kafka.bootstrap.servers = hadoop102:9092 hadoop103:9092 hadoop104:9092
     * a1.sinks.k1.kafka.topic = flume
     *
     * 2）说明：
     *  当topic没有指定时，如果消息的头部信息中含有以 "topic" 为key的kv时，将按照 "topic" 对应的value值为主题；
     *  否则，按照默认主题 "default-flume-topic"。
     *
     */
}
