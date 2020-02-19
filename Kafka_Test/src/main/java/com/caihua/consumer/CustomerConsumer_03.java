package com.caihua.consumer;

import org.apache.kafka.clients.consumer.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 3.自定义Consumer——手动提交Offset（同步）
 */
public class CustomerConsumer_03 {
    public static void main(String[] args) {
        //1.设置配置信息
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        //关闭自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
//        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        //2.创建Consumer
        Consumer<String,String> consumer = new KafkaConsumer<>(properties);

        //3.订阅主题
        consumer.subscribe(Arrays.asList("first"));

        //4.读取数据
        while(true){
            ConsumerRecords<String, String> polls = consumer.poll(100);
            for (ConsumerRecord<String, String> poll : polls) {
                System.out.println("Topic:" + poll.topic()
                            + "; Partition:" + poll.partition()
                            + "; Key:" + poll.key()
                            + "; value:" + poll.value());
            }
            //同步提交
            consumer.commitSync();
        }
    }
}
