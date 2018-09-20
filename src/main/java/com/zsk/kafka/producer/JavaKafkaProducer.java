package com.zsk.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Create by zsk on 2018/8/9
 **/
public class JavaKafkaProducer<KEY,VALUE> {
    private static final Logger logger = Logger.getLogger(JavaKafkaProducer.class);

    private String topicName;
    private String brokerList;
    public KafkaProducer<KEY,VALUE> producer=null;

    public JavaKafkaProducer(String topicName,String brokerList) {
       this.topicName=topicName;
       this.brokerList=brokerList;
       this.initialJavaProducer();
    }

    private void initialJavaProducer(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,this.brokerList);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"compression.type.snappy");
        this.producer=new KafkaProducer<KEY,VALUE>(properties);
    }
    public void close(){
        this.producer.close();
    }

    public String getTopicName() {
        return topicName;
    }
    public void sendMessage(ProducerRecord<KEY, VALUE> producerRecord){
          this.producer.send(producerRecord);
    }

}
