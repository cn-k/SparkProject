package cache;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.col;

public class Test {
    public static void main(String[] args) throws InterruptedException {
        String env_var = System.getenv("ENV_VAR");
        SparkConf conf = new SparkConf()
                .setAppName("Simple App")
                .setMaster("local[1]");
        //.set("spark.driver.bindAddress", "127.0.0.1");
        //String logFile = "/opt/spark/examples/jars/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        while (true) {
            var airportDS = spark.read().
                    format("csv").
                    option("header", "false").
                    load("in/airports.text");
            airportDS.persist(StorageLevel.MEMORY_ONLY());
            var madang = airportDS.filter(col("_c1").equalTo("Madang").or(col("_c1").equalTo("Nuuk")));
            ArrayList<Dataset<Row>> datasetList = new ArrayList<>();
            datasetList.add(airportDS);
            madang.persist();
            madang.show();
            //airportDS.show();
            //airportDS.printSchema();
            Thread.sleep(4000);
            //datasetList.get(0).unpersist();
            //Thread.sleep(3000);
        }
    }
}
