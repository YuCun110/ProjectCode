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
public class CustomerInterceptor_02 implements ProducerInterceptor<String,String> {
    private int success = 0;
    private int error = 0;
    //1.获取配置信息
    @Override
    public void configure(Map<String, ?> configs) {

    }
    //2.用于封装进KafkaProducer.send方法中，确保在消息被序列化以及计算分区前调用该方法
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        return record;
    }
    //3.该方法会在消息从RecordAccumulator成功发送到Kafka Broker之后，或者在发送过程中失败时调用。
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        //计算数据发送成功、失败的条数
        if(exception == null){
            success++;
        }else{
            error++;
        }
    }

    @Override
    public void close() {
        //打印数据发送成功和失败的次数
        System.out.println("成功：" + success + "次，失败：" + error + "次！");
}

}
