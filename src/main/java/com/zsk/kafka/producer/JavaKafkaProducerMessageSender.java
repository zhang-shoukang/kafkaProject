package com.zsk.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Create by zsk on 2018/8/9
 **/
public class JavaKafkaProducerMessageSender {
    public static class MessageSender implements Runnable{
        private JavaKafkaProducer<String,String> producer=null;
        private AtomicBoolean running= null;

        MessageSender(JavaKafkaProducer<String,String> producer,AtomicBoolean running){
            this.producer=producer;
            this.running=running;
        }
        @Override
        public void run() {
            String topicName = this.producer.getTopicName();
            while (this.running.get()){
                ProducerRecord<String, String> pr = JavaKafkaProducerMessageSender.generateKeyedMessage(topicName);
                this.producer.sendMessage(pr);
            }
            System.out.println(Thread.currentThread().getName()+"完成数据输出");
        }
    }
    public static void sendMessages(JavaKafkaProducer<String, String> producer, ExecutorService pool, int numThreads, AtomicBoolean running){
        for (int i=0;i<numThreads;i++){
            pool.submit(new MessageSender(producer,running));
        }
    }
    private static ThreadLocalRandom random = ThreadLocalRandom.current();
    private static char[] chs = "abcdefghigklmnopqrstuvwxyz".toCharArray();

    public static ProducerRecord<String,String> generateKeyedMessage(String topicName){
        StringBuilder stringBuilder = new StringBuilder();
        String key = "key_"+random.nextInt(10,100);
        int wordCount = random.nextInt(1, 5);
        for (int i=0;i<wordCount;i++){
            String word = generateKeyWord(random.nextInt(3,20));
            stringBuilder.append(word);
        }
        String message = stringBuilder.toString().trim();
        return new ProducerRecord<String,String>(topicName,key,message);
    }
    public static String generateKeyWord(int nums){
        StringBuilder stringBuilder = new StringBuilder();
        for (int i=0;i<nums;i++){
            stringBuilder.append(chs[random.nextInt(chs.length)]);
        }
        return stringBuilder.toString();
    }


}
