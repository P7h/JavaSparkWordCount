import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public final class JavaWordCount {

    public static void main(final String[] args) throws Exception {

        final SparkConf sparkConf = new SparkConf().setAppName("SparkJavaWordCount");
        final JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        final JavaRDD<String> lines = jsc.textFile("hdfs://localhost:9000/user/hduser/input", 1);

        final JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")));
        final JavaPairRDD<String, Integer> counts = words
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);

        //Sort on the key.
        counts.sortByKey().saveAsTextFile("hdfs://localhost:9000/user/hduser/output_spark");
        jsc.stop();
    }
}