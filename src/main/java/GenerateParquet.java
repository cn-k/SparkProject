import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GenerateParquet {
    public static void main(String[] args) {
        String env_var = System.getenv("ENV_VAR");
        SparkConf conf = new SparkConf()
                .setAppName("Simple App")
                .setMaster("local[1]");
        //.set("spark.driver.bindAddress", "127.0.0.1");
        String logFile = "/opt/spark/examples/jars/README.md"; // Should be some file on your system
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        final Random rnd = new Random();
        List<Kpi> data = IntStream.range(0, 10_000)
                .mapToObj(index -> new Kpi(rnd.nextDouble(), "X"))
                .collect(Collectors.toList());
        Dataset<Row> inputDataset = session.createDataFrame(data, Kpi.class);
        //inputDataset.show();
        session.sqlContext().setConf("spark.sql.parquet.compression.codec","uncompressed");
        inputDataset.write().format("parquet").save("/home/cenk/Desktop/projects/java/SparkProject/out/parq");
    }
}