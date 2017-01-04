package org.amit.testpipeline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Author amittank.
 *
 * This is a different approach at consuming kafka messages and using Spark to process it
 * and save processed data to HDFS.(Without using spark streaming.) Here I am creating a list of string from consumed
 * messages and feeding it to spark for processing.
 *
 */
public class ProcessData implements Serializable{

    /**
     * Pass the query string and path where data will be saved in HDFS
     * @param query - String to look for in kafka messages
     * @param pathString - Path where the result data from Spark will be saved in HDFS.
     * @return
     * @throws IOException
     */
    public boolean processKafkaData(String query, String pathString) throws IOException {
        //Get the consumer and Spark context.
        Consumer consumer = getConsumer();
        consumer.subscribe(Arrays.asList("test1"));
        JavaSparkContext sc = getSparkContext();
        List<String> recordList = new ArrayList<String>();
        System.out.println("Starting consuming data");
        while(true){
            //Consume records.
            ConsumerRecords<String, String> consumerRecord = consumer.poll(1000);
            for(ConsumerRecord record : consumerRecord){
                recordList.add(record.value().toString());
            }
            System.out.println("Got list of data from batch");
            JavaRDD<String> rdd = sc.parallelize(recordList);
            //Process consumed data and save it to HDFS
            processDataForQuery(rdd, recordList, query, pathString);
        }
    }

    /**
     * This method processes consumed data from kafka
     * @param rdd
     * @param recordList
     * @param query
     * @param pathString
     * @throws IOException
     */
    private void processDataForQuery(JavaRDD rdd, List<String> recordList, final String query, String pathString) throws IOException {
        JavaRDD filteredData = rdd.filter(new Function<String, Boolean>(){

            @Override
            public Boolean call(String s) throws Exception {
                return s.contains(query);
            }
        });

        List<String> resultData = filteredData.collect();
        // HDFS data stream.
        FSDataOutputStream out = createHDFSDataStream(pathString);
        System.out.println("Processed data in RDD and writing data to HDFS");
        for(String value : resultData){
            out.writeBytes(value);
        }
        out.close();
    }

    /**
     * Create kafka consumer configuration.
     */
    public Consumer getConsumer(){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("from.beginning", "true");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        System.out.println("Got consumer config");
        return consumer;
    }

    /**
     * Create java spark context
     */
    private JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf().setAppName("testPipeline").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

    /**
     * Create HDFS data stream
     * @param pathString
     * @return
     * @throws IOException
     */
    private FSDataOutputStream createHDFSDataStream(String pathString) throws IOException {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000");
        Path path = new Path(pathString);
        FileSystem fileSystem = FileSystem.get(hadoopConf);
        FSDataOutputStream out;
        fileSystem.setReplication(path, (short) 1);
        if(fileSystem.exists(path)){
            out = fileSystem.append(path);
        }else{
            out  = fileSystem.create(path);
        }
        return out;
    }
}
