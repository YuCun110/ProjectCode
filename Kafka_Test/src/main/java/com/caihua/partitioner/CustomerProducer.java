package com.caihua.partitioner;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class CustomerProducer {
    public static void main(String[] args) {
        //1.配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //ACK应答
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        //重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,2);
        //批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        //等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        //RecordAccumulator缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        //序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //2.设置自定义分区
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.caihua.partitioner.CustomerPartitioner");

        //3.创建一个生产者对象
        Producer<String,String> producer = new KafkaProducer<String, String>(properties);

        //4.将每条数据封装为ProducerRecord，并发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first", "mie" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        System.out.println("主题：" + metadata.topic()
                                + "、分区：" + metadata.partition()
                                + "、偏移量：" + metadata.offset()
                                + "；");
                    }else {
                        exception.printStackTrace();
                    }
                }
            });
        }

        //5.关闭
        producer.close();
    }
}
