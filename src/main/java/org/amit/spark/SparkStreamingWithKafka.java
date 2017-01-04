package org.amit.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Author amittank.
 *
 * Example of Spark Streaming.
 * We will be consuming messages(Los Angeles Crime data in this case) from a kafka topic every 2 seconds
 * and using Spark we will be filtering the crime incidents in Hollywood.
 *
 * This is a very basic example of traditional streaming/real time data processing application.
 */
public class SparkStreamingWithKafka {

    public static void main(String[] args) throws InterruptedException {
        //Spark configuration for Streaming
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingWithKafka").setMaster("local[4]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, new Duration(2000));

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put("sparkStreaming", 1);
        //Passing Kafka configuration and Map of topics to get Stream of data
        JavaPairReceiverInputDStream<String, String> javaReceiverPairInputDStream = KafkaUtils.createStream(javaStreamingContext,"localhost:2181","test-consumer-group", topicMap);
        //Mapping Stream of data to lines
        JavaDStream<String> lines = javaReceiverPairInputDStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws Exception {
                return tuple2._2;
            }
        });
        //Filtering lines to have Hollywood incident.
        JavaDStream<String> filteredLines = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("Hollywood");
            }
        });

        filteredLines.print();
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
