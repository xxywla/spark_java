import model.Point2;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.function.Consumer;

public class ReadCSV {
    public static void main(String[] args) {
        String filePath = args[0];
        SparkConf sparkConf = new SparkConf().setAppName("ReadCSV");
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
                    return new Point2();
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
        points.take(3).forEach(new Consumer<Point2>() {
            @Override
            public void accept(Point2 point) {
                System.out.println(point);
            }
        });
    }
}
