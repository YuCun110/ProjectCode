package com.caihua.producer.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.ArrayList;
import java.util.List;

/**
 * 需求：使用flume接收数据，并给每条数据添加前缀，输出到控制台。
 * 前缀可从flume配置文件中配置。
 */
public class CustomerSource extends AbstractSource implements Configurable, PollableSource {
    String prefix;
    String subfix;
    private Long delay;
    /**
     * 1.读取配置文件
     */
    @Override
    public void configure(Context context) {
        //1）获取配置信息
        //前后缀名
        prefix = context.getString("prefix", "hello-");//有默认值的配置信息
        subfix = context.getString("subfix");//默认值的配置信息
        //超时时间
        delay = context.getLong("delay",500L);
    }

    /**
     * 2.读取数据，创建Event，并发送至Channel
     */
    @Override
    public Status process() throws EventDeliveryException {
        //返回值状态
        Status status;
        List<Event> list = new ArrayList<>();
        try {
            //1）读取数据
            for (int i = 0; i < 10; i++) {
                //封装event
                SimpleEvent sEvent = new SimpleEvent();
                sEvent.setBody((prefix + "flume" + i + subfix).getBytes());
                //将事件添加到集合
                list.add(sEvent);
            }
            //2）批量提交
            getChannelProcessor().processEventBatch(list);
            //3）睡眠
            Thread.sleep(delay);
            //4)更新状态
            status = Status.READY;
        } catch (InterruptedException e) {
            e.printStackTrace();
            status = Status.BACKOFF;
        }
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
