package com.zsk.kafka.producer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Create by zsk on 2018/8/9
 **/
public class JavaKafkaProducerMessageSenderTest {
    public static void main(String[] args) throws Exception{
        test1();
    }
    public static void test1() throws Exception{

        String topicName="topic1";
        String bootstrapServers="47.100.184.77:9092";
        JavaKafkaProducer<String, String> producer = new JavaKafkaProducer<String, String>(topicName,bootstrapServers);
        AtomicBoolean running = new AtomicBoolean(true);
        int threadnumbs=1;
        ExecutorService pool = Executors.newFixedThreadPool(threadnumbs);
        JavaKafkaProducerMessageSender.sendMessages(producer,pool,threadnumbs,running);

        Thread.sleep(6000);
        running.set(false);
        producer.close();
        pool.shutdown();
    }
}
