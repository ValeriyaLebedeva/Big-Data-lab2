package lab3;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


public class AirportApp {
    public static final int NUM_DELAY_TIME = 18;
    public static final int TAIL_NUM = 9;
    public static final int FL_NUM = 10;
    public static final int ORIGIN_AIRPORT_ID = 11;
    public static final int DEST_AIRPORT_ID = 14;
    public static final int CANCELLED = 19;
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> linesTime = sc.textFile("/user/val/time_data.csv");
        JavaRDD<String> linesDesc = sc.textFile("/user/val/desc_data.csv");
        JavaPairRDD<String, String> glossary = linesDesc.map(s -> s.split(",")).mapToPair(
                s -> new Tuple2<>(removeQuotes(s[0]), removeQuotes(s[1]))
        );

        JavaPairRDD<String, String> timeDelayFlight = linesTime.map(s -> s.split(",")).mapToPair(
                s -> new Tuple2<>(removeQuotes(s[ORIGIN_AIRPORT_ID])+ ";" +removeQuotes(s[DEST_AIRPORT_ID]), s[NUM_DELAY_TIME])
        );
        Broadcast<List<Tuple2<String, String>>> g = sc.broadcast(glossary.collect());
        Tuple2<String, String> sqwer = g.value().get("44");
        JavaPairRDD<String, String> timeDelayMax = timeDelayFlight.groupByKey().mapValues(AirportApp::getMaxTime);
        JavaPairRDD<String, String> timeDelayMaxOut = timeDelayMax.mapToPair(s ->
                new Tuple2<>(glossary.lookup(s._1.split(";")[0]).get(0)+"; "+glossary.lookup(s._1.split(";")[1]).get(0), s._2)
//                        new Tuple2<>(s._1.split(";")[0]+"; "+s._1.split(";")[1], s._2)

        );

        timeDelayMaxOut.saveAsTextFile("output");

//        String resOut = linesTime.collect().toString();
//        System.out.println(resOut);



    }

    private static String getMaxTime(Iterable<String> masTime) {
        String maxTime = "-1.00";
        for (String t : masTime) {
            if ( !(t.isEmpty()) && Float.parseFloat(maxTime) < (Float.parseFloat(t))) {
                maxTime = t;
            }
        }
        System.out.println("max: " + maxTime);
        return maxTime;
    }

    private static String removeQuotes(String s) {
        return s.replace("\"", "");
    }
}
