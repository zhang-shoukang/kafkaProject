package com.zsk.kafka.consumer;

import com.zsk.kafka.constant.Constant;
import com.zsk.kafka.producer.KafkaProducerTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * Create by zsk on 2018/8/10
 **/
public class KafkaConsumerTest extends Thread{
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(KafkaProducerTest.class);
    public static final  String ZK = Constant.ZK;
    public static final String TOPIC = Constant.TOPIC;
    public static final String BROKER_LIST = Constant.BROKER_LIST;
    public static final String GROUP_ID = Constant.GROUP_ID;
    private String topic;
    KafkaConsumerTest(String topic){
        this.topic=topic;
    }
    private KafkaConsumer createConsume(){
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(MAX_POLL_RECORDS_CONFIG,10);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer(properties);

    }
    @Override
    public void run() {
        KafkaConsumer consume = createConsume();
        consume.subscribe(Arrays.asList(this.topic));
        boolean flag=true;
        while (true){
            ConsumerRecords<String,String> poll = consume.poll(100);

            if (poll.equals(ConsumerRecords.empty())){
                System.out.println(" log");
            }
            for (ConsumerRecord<String,String> record:poll){
                System.out.printf("partition=%d ,offset = %d, key = %s, value = %s%n", record.partition(),record.offset(), record.key(), record.value());
            }
        }
    }
}
