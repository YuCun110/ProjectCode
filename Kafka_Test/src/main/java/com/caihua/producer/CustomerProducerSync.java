package com.caihua.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class CustomerProducerSync {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.配置参数
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        //RecordAccumulator缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        //序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //2）创建Producer对象
        Producer<String,String> producer = new KafkaProducer<>(properties);
        //3）封装ProducerRecord对象（同步发送）
        for (int i = 0; i < 10; i++) {
            RecordMetadata future = producer.send(new ProducerRecord<String, String>("first", "superman" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.topic() + "--分区:" + metadata.partition() + ";--偏移量" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            }).get();
        }
        //4）关闭
        producer.close();
    }
}
