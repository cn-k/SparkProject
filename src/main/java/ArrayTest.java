import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;


public class ArrayTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Simple App")
                .setMaster("local[1]");
        //.set("spark.driver.bindAddress", "127.0.0.1");

        String logFile = "/opt/spark/examples/jars/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        List<String> salt_array1 = Arrays.asList("0", "1", "2");
        List<String> salt_array2 = Arrays.asList("3", "4");

        var ds1 = spark.createDataset(salt_array1, Encoders.STRING());

        var arry1 = ds1.withColumn("array1",  functions.array(salt_array1.stream().map(functions::lit).toArray(Column[]::new)));
        var arry2 = arry1.withColumn("array2",  functions.array(salt_array2.stream().map(functions::lit).toArray(Column[]::new)));
        arry2.printSchema();
        arry2.show();
        var res = arry2.withColumn("array_union", array_union(col("array1"), col("array2")));
        res.printSchema();
        res.show();
    }
}
