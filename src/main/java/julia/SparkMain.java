package julia;

import com.asml.api.JuliaJNIlib;
import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.col;

public class SparkMain {
    private static final String EXPECTED_MEAN_3_STDDEV_SPARK_SQL = "SELECT ABS(AVG(`_c0`)) + 3 * NVL(STDDEV(`_c0`), 0) as stddev" + System.lineSeparator() + "FROM `test_table`";

    public static void main(String[] args) {
        var dataPath = args[0];
        var libPath = args[1];
        SparkSession spark = SparkSession
                .builder().master("local[*]")
                .getOrCreate();
        var schema = new StructType(new StructField[]{new StructField("value", DataTypes.DoubleType, false, Metadata.empty())});
        var ds = spark.read()
                .schema(schema)
                //"/home/cnk/spark/statisticsData.csv"
                .csv(dataPath).repartition(100, new Column("value")).select(col("value").cast("int"));
        ds.show();
        ds.map((MapFunction<Row, Integer>) c -> JuliaJNIlib.callDouble(c.getInt(0), libPath), Encoders.INT()).show();
        //ds.map((MapFunction<Row, Integer>) row -> row.<Integer>getAs("value"), Encoders.INT());

    }
}
