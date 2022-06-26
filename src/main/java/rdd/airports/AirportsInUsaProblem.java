package rdd.airports;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.glassfish.jersey.message.internal.Utils;

public class AirportsInUsaProblem {
    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("AirportsInUsaProblem")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airports = sc.textFile("In/airports.text",3);
        var airportsInUSA = airports.filter(line -> line.split(COMMA_DELIMITER)[3].equals("\"United States\""));
        //JavaRDD<String> words = airports.filter(a -> a.split(",")[3].equals("United States"));
        //airportsInUSA.collect().forEach(System.out::println);
        var airportsInUSAArray = airportsInUSA.map(aiusa -> aiusa.split(COMMA_DELIMITER));
        var airportsNameAndCities = airportsInUSAArray.map(aiusa -> aiusa[1] + "," +aiusa[2]);
        airportsNameAndCities.collect().forEach(System.out::println);
    }
}
