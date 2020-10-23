package lab3;
import org.apache.spark.api.java.JavaRDD;


public class AirportApp {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf(Hadoop ).setAppName(Hadoop "example");
        JavaSparkContext sc = new JavaSparkContext(Hadoop conf);
        JavaRDD<String> distFile = sc.textFile(Hadoop "war-and-peace-1.txt");
    }
}
