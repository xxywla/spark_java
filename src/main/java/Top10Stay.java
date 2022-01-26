import model.Point2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public class Top10Stay {
    public static void main(String[] args) {
        String filePath = args[0];
        final float lon_min = Float.parseFloat(args[2]);
        final float lon_max = Float.parseFloat(args[3]);
        final float lat_min = Float.parseFloat(args[4]);
        final float lat_max = Float.parseFloat(args[5]);
        SparkConf sparkConf = new SparkConf().setAppName("RectRangeQuery");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> tra = sc.textFile(filePath);
        JavaRDD<Point2> points = tra.map(new Function<String, Point2>() {
            @Override
            public Point2 call(String s) throws Exception {
                String[] list = s.split(",");
                for (int i = 0; i < list.length; i++) {
                    int len = list[i].length();
                    list[i] = list[i].substring(1, len - 1);
                }
                String planNumber = list[0];
                if (planNumber.equals("plan_no")) {
                    return null;
                }
                String truckNumber = list[1];
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                Date utc = simpleDateFormat.parse(list[2]);
                float latitude = Float.parseFloat(list[3]);
                float longitude = Float.parseFloat(list[4]);
                Point2 point = new Point2(longitude, latitude, utc, planNumber, truckNumber);
                return point;
            }
        });
        JavaRDD<Point2> pointsRange = points.filter(new Function<Point2, Boolean>() {
            @Override
            public Boolean call(Point2 point2) throws Exception {
                if (point2 == null) {
                    return false;
                }
                float lon = point2.getLongitude();
                float lat = point2.getLatitude();
                if (lon >= lon_min && lon <= lon_max && lat >= lat_min && lat <= lat_max) {
                    return true;
                }
                return false;
            }
        });
        JavaPairRDD<String, Date> plan_utc = pointsRange.mapToPair(new PairFunction<Point2, String, Date>() {
            @Override
            public Tuple2<String, Date> call(Point2 point2) throws Exception {
                return new Tuple2<>(point2.getPlanNumber(), point2.getUtc());
            }
        });
        JavaPairRDD<String, Date> plan_utc_max = plan_utc.reduceByKey(new Function2<Date, Date, Date>() {
            @Override
            public Date call(Date date, Date date2) throws Exception {
                if (date2.after(date)) {
                    return date2;
                }
                return date;
            }
        });
        JavaPairRDD<String, Date> plan_utc_min = plan_utc.reduceByKey(new Function2<Date, Date, Date>() {
            @Override
            public Date call(Date date, Date date2) throws Exception {
                if (date2.before(date)) {
                    return date2;
                }
                return date;
            }
        });
        Map<String, Date> mp_max = plan_utc_max.collectAsMap();
        Map<String, Date> mp_min = plan_utc_min.collectAsMap();
        Map<String, Long> map = new TreeMap<String, Long>();
        for (Map.Entry<String, Date> item : mp_max.entrySet()) {
            map.put(item.getKey(), mp_max.get(item.getKey()).getTime() - mp_min.get(item.getKey()).getTime());
        }
        List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                return (int) (o2.getValue() - o1.getValue());
            }
        });
        int k = Math.min(10, list.size());
        for (int i = 0; i < k; i++) {
            System.out.println(list.get(i).toString());
        }
    }
}
