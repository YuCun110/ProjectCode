package com.caihua.consumer;

import org.apache.kafka.clients.consumer.*;

import java.util.Collections;
import java.util.Properties;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 1.自定义Consumer——自动提交offset
 */
public class CustomerConsumer_01 {
    public static void main(String[] args) {
        //1.定义配置信息
        Properties properties = new Properties();
        //1）指定连接的集群信息
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //2）指定消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        //3）设置自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        //4）自动提交的延时（自动提交offset的时间间隔）
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        //5）反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        //2.创建Condumer对象
        Consumer<String,String> consumer = new KafkaConsumer<>(properties);

        //3.订阅主题
        consumer.subscribe(Collections.singletonList("first"));

        //4.消费数据(轮询)
        while(true){
            //设置超时时间（延时，拉取数据的时间间隔），拉取一条数据
            ConsumerRecords<String, String> polls = consumer.poll(100);
            for (ConsumerRecord<String, String> poll : polls) {
                System.out.println("主题Topic：" + poll.topic()
                        + "；分区Partition：" + poll.partition()
                        + "；Key：" + poll.key()
                        + "；Value：" + poll.value()
                        + "；偏移量Offset：" + poll.offset());
            }
        }
    }
}
