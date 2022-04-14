import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;

public class Register {
    public static void main(String[] args) {
        String env_var = System.getenv("ENV_VAR");
        SparkConf conf = new SparkConf()
                .setAppName("Simple App")
                .setMaster("local[1]");
        //.set("spark.driver.bindAddress", "127.0.0.1");
        //String logFile = "/opt/spark/examples/jars/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        spark.udf().register("timesTwo", (Integer x) -> x*2, DataTypes.IntegerType);
        spark.sql("SELECT timesTwo(2)").show();
        spark.sql("SHOW FUNCTIONS LIKE 'time*';").show();
    }
}
