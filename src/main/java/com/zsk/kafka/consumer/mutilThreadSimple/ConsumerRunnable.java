package com.zsk.kafka.consumer.mutilThreadSimple;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.PatternLayout;
import scala.actors.threadpool.Arrays;

import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Create by zsk on 2018/9/19
 **/
public class ConsumerRunnable implements Runnable {
    private final KafkaConsumer<String,String> consumer;
    public ConsumerRunnable(String brokerList,String groupId,String topic){
        Properties properties = new Properties();
        properties.put("bootstrap.servers",brokerList);
        properties.put("group.id",groupId);
        properties.put("enable.auto.commit",true);
        properties.put("auto.commit.interval.ms","3000");
        properties.put("session.timeout.ms","30000");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer= new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Pattern.compile(topic));
    }
    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Integer.MAX_VALUE);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(Thread.currentThread().getName() + " consumed " + record.partition() + " the message with offset " + record.offset());
            }
        }
    }
}
