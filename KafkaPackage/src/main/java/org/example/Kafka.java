package org.example;

// import org.apache.kafka.clients.admin.Admin;

import org.apache.kafka.clients.admin.AdminClient;
// import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Kafka {

    private String topic;
    private String message;
    private int partitions;
    private String key;
    public Kafka(String topic, int partitions, String message,String key) throws ExecutionException, InterruptedException {
        this.topic = topic;
        this.message = message;
        this.partitions = partitions;
        this.key = key;
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

//        try (Admin admin = Admin.create(props)) {
//            int partitions = 1;
//            short replicationFactor = 1;
//            NewTopic newTopic = new NewTopic(topic, partitions, replicationFactor);
//
//            CreateTopicsResult result = admin.createTopics(
//                    Collections.singleton(newTopic)
//            );
//
//        }

        AdminClient admin = AdminClient.create(props);

        //checking if topic already exist
        boolean alreadyExist = admin.listTopics().names().get().stream().anyMatch(existingTopicName -> existingTopicName.equals(topic));
        if (alreadyExist){
            System.out.println("Topic already exist"+topic);
        }
        else{
            NewTopic newTopic = new NewTopic(this.topic,this.partitions, (short) 1);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }


        ProducerRecord producerRecord = new ProducerRecord(this.topic,this.key,this.message);
        KafkaProducer kafkaProducer = new KafkaProducer(props);
        kafkaProducer.send(producerRecord);
        kafkaProducer.close();
    }
}

