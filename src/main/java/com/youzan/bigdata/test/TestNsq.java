package com.youzan.bigdata.test;

import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class TestNsq {

    public static void main(String[] avgs){
        System.out.println("aa");
        final NSQConfig config = new NSQConfig();
        config.setLookupAddresses("nsq-pre.s.qima-inc.com:4161");
        config.setConsumerName("nsqdata2kakfa");
        config.setConnectTimeoutInMillisecond(1000);

        Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                String msg = message.getReadableContent();
                System.out.println(msg);

            }
        });

        try {
            System.out.println("bb");
            consumer.subscribe(new Topic("ntc_order_create"));
            consumer.setAutoFinish(false);
            consumer.start();
            System.out.println("cc");
        }catch (NSQException e){
            e.printStackTrace();
        }
    }
}