package explode;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class KnownLang {
    public static void main(String[] args) {

        /*
    import spark.implicits._
        String env_var = System.getenv("ENV_VAR");
        SparkConf conf = new SparkConf()
                .setAppName("Simple App")
                .setMaster("local[1]");
        //.set("spark.driver.bindAddress", "127.0.0.1");
        String logFile = "/opt/spark/examples/jars/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        var arrayData = Set(
                Row("James", List("Java", "Scala"), Map("hair"->"black", "eye"->"brown")),
        Row("Michael", List("Spark", "Java", null), Map("hair"->"brown", "eye"->null)),
        Row("Robert", List("CSharp", ""), Map("hair"->"red", "eye"->"")),
        Row("Washington", null, null),
                Row("Jefferson", List(), Map())
    );

        var arraySchema = new StructType()
                .add("name", StringType)
                .add("knownLanguages", ArrayType(StringType))
                .add("properties", MapType(StringType, StringType));

        var df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema)
        df.printSchema()
        df.show(false)

         */
    }
}
