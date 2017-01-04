package org.amit.testpipeline;

/**
 * Author amittank.
 *
 * Test method to push data to kafka producer.
 * We will have to run TestProcessData.java and TestPushData.java both together
 * to test the netire flow of posting data to topic, consuming it, processing it,
 * and saving to HDFS.
 */
public class TestPushData {

    public static void main(String[] args){
        try{
            PushData pushData = new PushData();
            pushData.readFileAndPushToKafka("/Users/Crimes_2012-2015.csv","test1");

            System.out.print("Data pushed to Kafka topic");

        }catch(Exception exception){

        }

    }
}
