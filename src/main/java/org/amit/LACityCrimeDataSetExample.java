package org.amit;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.util.List;

/**
 * Author amittank
 *
 * This simple example reads a csv file of crime data in Los Angeles from a source location. (In this case from the local storage)
 * and finds crime incident in hollywood and print the records on console as well as store data in hdfs.
 *
 * The code is more focused on functionality than programming practices.
 * We can enhance it to handle exceptions differently.
 *
 * To run this example: Pass below arguments as a program arguments
 * args[0] - CSV File location
 * args[1] - string to find for
 * args[2] - hdfs URL
 * args[3] - location at which the result data to be stored in HDFS.
 */
public class LACityCrimeDataSetExample {

    public static void main(final String[] args) throws IOException {
        //Create spark configuration
        SparkConf conf = new SparkConf().setAppName("LACityCrimeDataSetExample").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Create Hadoop configuration
        Configuration hadoopConf = new Configuration();
        //hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000");
        hadoopConf.set("fs.defaultFS", args[2]);

        //Create path in HDFS where result data will be stored.
        //Path path = new Path("/input/hollywood.txt");
        Path path = new Path(args[3]);
        FileSystem fileSystem = FileSystem.get(hadoopConf);
        FSDataOutputStream out = fileSystem.create(path);

        try{
            //String file = "/Users/amittank/Downloads/Crimes_2012-2015.csv";
            String file = args[0];

            //Read the file as a text file and apply filter
            JavaRDD<String> lines = sc.textFile(file);
            JavaRDD<String > filteredLines = lines.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {
                    return s.contains(args[1]);
                }
            });

            //Collect the result and write to console and file.
            List<String> finalLines = filteredLines.collect();
            for(String s : finalLines) {
                System.out.println(s);
                out.write(s.getBytes());
            }
            System.out.print("Total count is : "+ finalLines.size());
        }catch(Exception exception){
            System.out.println("The following exception occured : " + exception);
        }finally {
            out.close();
            fileSystem.close();
            sc.close();
        }
    }
}
