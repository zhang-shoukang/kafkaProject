package com.zsk.kafka.producer;

import com.zsk.kafka.constant.Constant;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;
/**
 * Create by zsk on 2018/8/10
 **/
public class KafkaProducerTest  extends Thread{
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(KafkaProducerTest.class);
    public static final  String ZK = Constant.ZK;
    public static final String TOPIC = Constant.TOPIC;
    public static final String BROKER_LIST = Constant.BROKER_LIST;

    private String topic;
    private KafkaProducer<String,String> producer;

    public  KafkaProducerTest(String topic){
        this.topic = topic;

        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ACKS_CONFIG, "1");
        properties.put(LINGER_MS_CONFIG, 0);
        properties.put(BATCH_SIZE_CONFIG, 10);

        producer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (true) {
            String message = "message_" + messageNo;
            producer.send(new ProducerRecord<String, String>(topic, message), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                }
            });
            System.out.println(message);
            messageNo++;
        }
    }
}
