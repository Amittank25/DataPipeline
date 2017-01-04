package org.amit.testpipeline;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static jodd.util.ThreadUtil.sleep;

/**
 * Author amittank.
 *
 * This class reads a file and pushes the lines as a message to kafka topic(Producer).
 */
public class PushData {

    /**
     * Read a file from the provided filePath and push the lines to topicName passed to the method every 100ms
     * @param filePath
     * @param topicName
     * @throws IOException
     */
    public void readFileAndPushToKafka(String filePath, String topicName) throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        Producer producer = createKafkaProducer();
        Double count = 0D;
        System.out.println("Writing line");
        while(reader.readLine() != null){
            pushDataToKafkaProducer(producer,reader.readLine(), count, topicName);
            sleep(100);
        }
    }

    /**
     * Producer configuration
     * @return
     */
    private Producer createKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(props);
        System.out.println("Created Kafka Producer config");
        return producer;
    }

    /**
     * Publish message to kafka topic.
     * @param producer
     * @param data
     * @param count
     * @param topicName
     */
    private void pushDataToKafkaProducer(Producer producer, String data, Double count, String topicName) {
        producer.send(new ProducerRecord<String, String>(topicName, count.toString() ,data));
        System.out.println("Pushed data to topic");
    }
}
