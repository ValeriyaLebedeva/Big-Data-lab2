package lab3;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;


public class AirportApp {
    public static final int NUM_DELAY_TIME = 18;
    public static final int TAIL_NUM = 9;
    public static final int FL_NUM = 10;
    public static final int ORIGIN_AIRPORT_ID = 11;
    public static final int DEST_AIRPORT_ID = 14;
    public static final int CANCELLED = 19;
    private static final String FILE_SPLITTER = ",";
    public static final String DELIMITER_CSV = ";";
    public static final String DELIMITER_IDS = ";";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> linesTime = sc.textFile("/user/val/time_data.csv");
        JavaRDD<String> linesDesc = sc.textFile("/user/val/desc_data.csv");
        Map<String, String> glossaryAsMap = linesDesc.mapToPair(AirportApp::getParsedGlossary).collectAsMap();
        JavaPairRDD<String, String> timeDelayFlight = linesTime.map(s -> s.split(DELIMITER_CSV)).mapToPair(
                s -> new Tuple2<>(removeQuotes(s[ORIGIN_AIRPORT_ID])+ DELIMITER_IDS +removeQuotes(s[DEST_AIRPORT_ID]), s[NUM_DELAY_TIME])
        );
        JavaPairRDD<String, String> cancelledFlight = linesTime.map(s -> s.split(DELIMITER_CSV)).mapToPair(
                s -> new Tuple2<>(removeQuotes(s[ORIGIN_AIRPORT_ID])+ DELIMITER_IDS +removeQuotes(s[DEST_AIRPORT_ID]), s[CANCELLED])
        );
        Broadcast <Map<String, String>> glossaryAsBroadcast = sc.broadcast(glossaryAsMap);
        JavaPairRDD<String, String> timeDelayMax = timeDelayFlight.groupByKey().mapValues(AirportApp::getMaxTime);
        JavaPairRDD<String, String> percentCancelled = cancelledFlight.groupByKey().mapValues(AirportApp::getPercentUnderZero);
        JavaPairRDD<String, String> percentDelay = timeDelayFlight.groupByKey().mapValues(AirportApp::getPercentUnderZero);
        JavaPairRDD<String, Tuple2<Tuple2<String, String>, String>> gh = timeDelayMax.join(percentDelay).join(percentCancelled);
        JavaPairRDD<String, String> idsAndData = gh.mapValues(AirportApp::convertTuplesToString);
        JavaPairRDD<String, String> descriptionsAndData = idsAndData.mapToPair(s ->
                new Tuple2<>(glossaryAsBroadcast.value().get(s._1.split(DELIMITER_CSV)[0])+"; "+glossaryAsBroadcast.value().get(s._1.split(DELIMITER_CSV)[1]), s._2)
        );
        descriptionsAndData.saveAsTextFile("output");

    }


//    private static JavaPairRDD<String, String> h(JavaPairRDD<String, String> obj, Function<Iterable<String>, String> getNecessaryStat) {
//        return obj.groupByKey().mapValues(getNecessaryStat::apply);
//    }

    private static String convertTuplesToString(Tuple2<Tuple2<String, String>, String> s) {
        return "Max time delay: " + s._1._1 + "; Delay percent: " + s._1._2 + "; Cancelled percent: " + s._2() + ";";
    }


    private static Tuple2<String, String> getParsedGlossary(String str) {
        int numSplitter = str.indexOf(FILE_SPLITTER);
        String idAirport = str.substring(1, numSplitter-1);
        String description = str.substring(numSplitter+1);
        return new Tuple2<>(removeQuotes(idAirport), removeQuotes(description));
    }

    private static String getPercentUnderZero(Iterable<String> mark) {
        int allCount = 0;
        int underZero = 0;
        for (String m : mark) {
            if ( !(m.isEmpty()) && Float.parseFloat(m) > 0) {
                underZero++;
            }
            allCount++;
        }
        if (allCount!=0) {
            return String.valueOf(underZero/(float)allCount*100);
        } else {
            return "0";
        }
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
