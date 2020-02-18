package com.caihua.consumer;

import org.apache.kafka.clients.consumer.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 2.自定义Consumer——自动提交Offset--from-beginning
 */
public class CustomerConsumer_02 {
    public static void main(String[] args) {
        //1.设置配置信息
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test03");
        //自定义手动提交--from-begining
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//earliest;latest;
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        //2.创建Consumer对象
        Consumer<String,String> consumer = new KafkaConsumer<>(properties);

        //3.订阅主题
        consumer.subscribe(Arrays.asList("second","first"));

        //4.读取数目（轮询）
        while(true){
            //读取一次数据，设置超时时间
            ConsumerRecords<String, String> polls = consumer.poll(100);
            for (ConsumerRecord<String, String> poll : polls) {
                System.out.println("Topic:" + poll.topic()
                            + "; Partition:" + poll.partition()
                            + "; Key:" + poll.key()
                            + "; Value:" + poll.value()
                            + "; Offset:" + poll.offset());
            }
        }
    }
}
