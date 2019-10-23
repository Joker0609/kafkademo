package com.example.kafkademo.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @version 0.0.1
 * @program: kafkademo
 * @description: kafka生产者
 * @packname: com.example.kafkademo.producer
 * @author: wzp
 * @create: 2019-10-23 10:24
 */
public class KafkaProducer extends Thread {
    private  Producer<Integer,String> producer;
    /**
     * 读取配置文件
     */
    private final Properties props = new Properties();
    /**
     * 指定当前kafka producer生产的数据目的地；
     *
     */
    private String topic = "test";

    public KafkaProducer(String topic) {
        /**
         * key.serializer.class默认为serializer.class
         */
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        /**
         * kafka broker对应的主机，格式为host1:port1,host2:port2
         */
        props.put("metadata.broker.list", "192.168.31.131:9091,192.168.31.131:9092,192.168.31.131:9093");
        props.put("zookeeper.connect", "l27.0.0.1:2181");
        /**
         * 通过配置文件，创建生产者
         */
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }
    public void run(){
        int messageNo = 1;
        while (true){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            String  messageStr= new String("Message_" + messageNo);
            System.out.print(messageStr);
            try {
                producer.send(new KeyedMessage<Integer, String>("topic", messageStr));
            }catch (Exception e){
                e.printStackTrace();
            }
            messageNo ++;
        }
    }
    public static void main(String[] args) {
        KafkaProducer producerThread = new KafkaProducer("topic");
        producerThread.start();
    }
}
