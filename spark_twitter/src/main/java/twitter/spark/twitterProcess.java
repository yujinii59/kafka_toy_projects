package twitter.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class twitterProcess {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("TwitterData")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        String dataDir = "data/twitterData.json";
        Dataset<Row> twitterDS = spark
                .read()
                .option("inferschema", "true")
                .json(dataDir);

        Dataset<Row> twitter = twitterDS
                .select(col("data.id").as("id"), col("data.text").as("text"));

        twitter
                .filter(expr("text not rlike '@'"))
                .withColumn("convText", regexp_replace(col("text"), "[ㄱ-ㅎ가-힣]", "@"))
                .filter(expr("convText rlike '@'"))

//                .withColumn(
//                "text",
//                        expr("filter(text, x -> x not rlike '@')")
////                        expr("filter(text, x -> x rlike '^[ㄱ-ㅎ|가-힣|a-z|A-Z]+$')")
//                )
                .show(false);
    }

}
