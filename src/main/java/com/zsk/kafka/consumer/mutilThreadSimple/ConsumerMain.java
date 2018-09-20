package com.zsk.kafka.consumer.mutilThreadSimple;

import com.zsk.kafka.constant.Constant;

/**
 * Create by zsk on 2018/9/19
 **/
public class ConsumerMain {
    public static void main(String[] args) {
        int threadNum = Runtime.getRuntime().availableProcessors();
        ConsumerGroup consumerGroup = new ConsumerGroup(threadNum,Constant.GROUP_ID,Constant.TOPIC,Constant.BROKER_LIST);
        consumerGroup.execute();
    }
}
