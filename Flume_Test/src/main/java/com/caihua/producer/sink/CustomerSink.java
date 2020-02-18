package com.caihua.producer.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义Sink：
 * 使用flume接收数据，并在Sink端给每条数据添加前缀和后缀，输出到控制台。前后缀可在flume任务配置文件中配置。
 */
public class CustomerSink extends AbstractSink implements Configurable {
    //声明前后缀
    private String prefix;
    private String subfix;
    //创建log对象
    private Logger logger = LoggerFactory.getLogger(CustomerSink.class);
    //1.读取任务配置文件中的配置信息
    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix", "flume");
        subfix = context.getString("subfix");
    }
    //2.从channel中取数据，添加前后缀，并写入日志
    @Override
    public Status process() throws EventDeliveryException {
        //1）定义状态信息
        Status status;
        //2）获取Channel
        Channel channel = getChannel();
        //3）从Channel获取事务
        Transaction tc = channel.getTransaction();
        //4）开启事务
        tc.begin();
        try {
            //5）从Channel中获取数据（注意try-catch）
            Event event = channel.take();
            if(event != null){
                //6）处理数据（打印到控制台）
                logger.info(prefix + new String(event.getBody())+ subfix);
            }
            //7）提交
            tc.commit();
            //8）更改状态信息
            status = Status.READY;
        } catch (ChannelException e) {
            //9）catch回滚，更改状态
            tc.rollback();
            status = Status.BACKOFF;

            e.printStackTrace();

        } finally {
            //10）finally 事务关闭
            tc.close();
        }
        return status;
    }
}
