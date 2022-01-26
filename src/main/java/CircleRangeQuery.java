import model.Point2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CircleRangeQuery {
    public static void main(String[] args) {
        String filePath = args[0];
        final float pLon = Float.parseFloat(args[2]);
        final float pLat = Float.parseFloat(args[3]);
        final double r = Float.parseFloat(args[4]);
        String savePath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("CircleRangeQuery");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        circleRangeQuery(sc, filePath, pLon, pLat, r, savePath);
    }

    public static void circleRangeQuery(JavaSparkContext sc, String filePath, float pLon, float pLat, double r, String savePath) {
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
        points.filter(new Function<Point2, Boolean>() {
            @Override
            public Boolean call(Point2 point2) throws Exception {
                if (point2 == null) {
                    return false;
                }
                float lon = point2.getLongitude();
                float lat = point2.getLatitude();
                if (Utils.calPoiToPoiDis(lon, lat, pLon, pLat) < r) {
                    return true;
                }
                return false;
            }
        }).saveAsTextFile(savePath);
    }
}
