package org.amit.testpipeline;

import java.io.IOException;

/**
 * Author amittank.
 *
 * Test method for kafka consumer.
 * We will have to run TestProcessData.java and TestPushData.java both together
 * to test the netire flow of posting data to topic, consuming it, processing it,
 * and saving to HDFS.
 */
public class TestProcessData {

    public static void main(String[] args){

        try {
            ProcessData processData = new ProcessData();
            processData.processKafkaData("Hollywood", "/input/hollywood2.txt");
            System.out.print("Data processed from Kafka topic and saved in HDFS");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
