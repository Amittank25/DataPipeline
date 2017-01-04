package org.amit.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;


/**
 * Author amittank.
 *
 * Demonstration of kafka producer and consumer configuration we have created.
 * In this example we will be reading a csv file and send each row of csv file as a producer record.
 * after pushing all the records we call consumer. Consumer will retrive the record and print it on console.
 *
 * We can pass hard coded file name as a program argument and can start producer and consumer together to see
 * actual messaging/streaming behavior.(Refer testpipeline) For simplicity we are starting producer first and then starting consumer.
 * Also, we can have our own processing logic in the producer and consumer part.
 */
public class KafkaOperations {

    public static void main(String[] args) throws IOException {

        String file = "/Bigdata/Data/State_Zhvi_Summary_AllHomes.csv";

        //Get producer instance and send records.
        Producer<String, String> producer = new KafkaProducerClass().getKafkaProducer();
        Double count = 0D;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                producer.send(new ProducerRecord<String, String>("test",(count++).toString(), line));
            }
        }catch (Exception exception){
            System.out.print("File read exception");
        }
        System.out.println("Message sent successfully");
        producer.close();

        //Call consumer to consume records.
        callConsumer();
    }

    private static void callConsumer() {
        //Get the consumer config and subscribe to topic
        Consumer consumer = new KafkaConsumerClass().getConsumer();
        consumer.subscribe(Arrays.asList("test"));

        //Retrive the records at an interval of 100ms and print it.
        while (true) {
            ConsumerRecords<String, String> records =  consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
            // print the offset,key and value for the consumer records.
            {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
            }
        }
    }
}
