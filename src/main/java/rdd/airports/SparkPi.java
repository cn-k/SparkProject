package rdd.airports;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;


public class SparkPi {

    public static void main(String[] args) {
        String env_var = System.getenv("ENV_VAR");
        SparkConf conf = new SparkConf()
                .setAppName("Simple App")
                .setMaster("local[1]");
                //.set("spark.driver.bindAddress", "127.0.0.1");


        String logFile = "/opt/spark/examples/jars/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
        long numBs = logData.filter((FilterFunction<String>) s -> s.contains("b")).count();
        System.out.println("ENV VAR : " +System.getenv("ENV_VAR"));
        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();

    }
}
