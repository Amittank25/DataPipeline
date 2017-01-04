package org.amit.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.util.Properties;


/**
 * Author amittank.
 *
 * Basic kafka producer. getKafkaProducer() provides kafka producer instance.
 * We can read these properties from a properties file too. Please see comments.
 */
public class KafkaProducerClass {

    public Producer getKafkaProducer() throws IOException {
        Properties properties = new Properties();

        //Below commented line of code read a properties file "producer.properties" and load the properties. You do not need the code below
        //these 2 line if you are loading properties from properties file.
        //InputStream inputStream = getClass().getClassLoader().getResourceAsStream("/Users/amittank/Documents/Amit/PersonalProjects/workspace/src/main/resources/producer.properties");
        //properties.load(inputStream);

        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("buffer.memory", 33554432);
        //Avro serializer can be configured similar way as below.
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(properties);
        return producer;
    }

}
