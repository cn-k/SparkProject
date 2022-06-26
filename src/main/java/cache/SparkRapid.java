package cache;

import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkRapid {
    //public static final String EXPECTED_MEAN_3_STDDEV_SPARK_SQL = "SELECT ABS(AVG(_c0)) + 3 * NVL(STDDEV(_c0), 0) from test_table";
    private static final String EXPECTED_MEAN_3_STDDEV_SPARK_SQL = "SELECT ABS(AVG(`_c0`)) + 3 * NVL(STDDEV(`_c0`), 0) as stddev" + System.lineSeparator() + "FROM `test_table`";


    public static void main(String[] args) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        SparkSession spark = SparkSession
                .builder()
                /*
                .config("spark.plugins","com.nvidia.spark.SQLPlugin")
                .config("spark.rapids.sql.incompatibleOps.enabled", true)
                //.config("spark.rapids.force.caller.classloader", false)
                .config("spark.executor.resource.gpu.amount", 100)
                .config("spark.task.resource.gpu.amount", 0.5)
            */
                .master("local[*]").getOrCreate();

        Dataset<Row> ds = spark.read().csv("/home/cnk/spark/statisticsData.csv").repartition(100, new Column("_c0"));
        ds.createOrReplaceTempView("test_table");
        spark.sql(EXPECTED_MEAN_3_STDDEV_SPARK_SQL).show() ;
        stopWatch.stop();
        System.out.println(stopWatch.getTime());
    }
}
