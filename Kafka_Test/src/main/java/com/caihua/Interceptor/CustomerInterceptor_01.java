package com.caihua.Interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 1.实现一个简单的双interceptor组成的拦截链。第一个interceptor会在消息发送前将时间戳信息加到消息value的最前部；
 *  第二个interceptor会在消息发送后更新成功发送消息数或失败发送消息数。
 */
public class CustomerInterceptor_01 implements ProducerInterceptor<String,String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        //return null;
        return new ProducerRecord<>(record.topic(),record.partition(),record.key(),
                System.currentTimeMillis() + "_" + record.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
