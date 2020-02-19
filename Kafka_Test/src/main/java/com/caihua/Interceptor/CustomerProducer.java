package com.caihua.Interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class CustomerProducer {
    public static void main(String[] args) {
        //1.获取所需的配置参数
        Properties properties = new Properties();
        //1）Kafka集群，broker—list
        properties.put("bootstrap.servers","hadoop102:9092");
        properties.put("acks","all");
        //2)重试次数
        properties.put("retries",3);
        //3）批次大小
        properties.put("batch.size",16384);
        //4）等待时间
        properties.put("linger.ms",1);
        //5）RecordAccumulater缓冲区大小
        properties.put("buffer.memory",33554432);
        //6）序列化
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //2.构建拦截链
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.caihua.Interceptor.CustomerInterceptor_01");
        interceptors.add("com.caihua.Interceptor.CustomerInterceptor_02");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        //2.创建一个生产者对象
        Producer<String,String> producer = new KafkaProducer<>(properties);

        //3.将每条数据封装成ProducerRecord对象，发送数据（异步发送）
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first",Integer.toString(0),"talk"+i));
        }
        //4.关闭
        producer.close();
    }
}
