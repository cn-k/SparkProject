package cache;

import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

public class SparkRapid {
    private static final String EXPECTED_MEAN_3_STDDEV_SPARK_SQL = "SELECT ABS(AVG(`_c0`)) + 3 * NVL(STDDEV(`_c0`), 0) as stddev" + System.lineSeparator() + "FROM `test_table`";

    public static void main(String[] args) {
        ArrayList<Long> list = new ArrayList<>();
        StopWatch stopWatch = new StopWatch();
        for (int i=0; i<10; i++){
            stopWatch.start();
            SparkSession spark = SparkSession
                    .builder()
                    .getOrCreate();

            Dataset<Row> ds = spark.read().csv("/home/cnk/spark/statisticsData.csv").repartition(100, new Column("_c0"));
            ds.createOrReplaceTempView("test_table");
            var res = spark.sql(EXPECTED_MEAN_3_STDDEV_SPARK_SQL);
            res.write().mode("overwrite").format("noop").save();
            stopWatch.stop();
            var duration = stopWatch.getTime();
            list.add(duration);
            stopWatch.reset();
        }
        var average = list.stream().reduce((x,y) -> x+y).get() / list.size();
        for (int i  = 1; i<=10 ; i++){
            System.out.println("iteration " + i + ": " + list.get(i-1));
        }
        System.out.println("average time is: " + average);
        try {
            Thread.sleep(1000000000000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
