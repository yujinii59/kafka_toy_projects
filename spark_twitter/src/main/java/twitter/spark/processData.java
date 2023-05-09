package twitter.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class processData {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("chatgptProcess")
            .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
                .option("subscribe", "chatgpt-twitter")
                .load();

        df.show();
        System.out.println("Test");
    }

}
