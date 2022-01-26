import javafx.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class KMeans {
    public static double distanceSquared(List<Integer> point1, List<Double> point2) {
        double sum = 0.0;
        for (int i = 0; i < point1.size(); i++) {
            sum += Math.pow(point1.get(i).doubleValue() - point2.get(i), 2);
        }
        return sum;
    }

    private static List<Integer> addPoints(List<Integer> p1, List<Integer> p2) {
        ArrayList<Integer> ret = new ArrayList<>();
        for (int i = 0; i < p1.size(); i++) {
            ret.add(p1.get(i) + p2.get(i));
        }
        return ret;
    }

    private static Integer closestPoint(List<Integer> point, List<List<Double>> kPoints2) {
        int bestIndex = 0;
        double cloest = Double.POSITIVE_INFINITY;
        for (int i = 0; i < kPoints2.size(); i++) {
            double dist = distanceSquared(point, kPoints2.get(i));
            if (dist < cloest) {
                cloest = dist;
                bestIndex = i;
            }
        }
        return bestIndex;
    }

    public static void run() {
        SparkConf conf = new SparkConf().setAppName("KMeans").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        int iterateNum = 20;
        JavaRDD<List<Integer>> points = sc.textFile("E:/data/kmeans/data.txt").map(new Function<String, List<Integer>>() {

            @Override
            public List<Integer> call(String s) throws Exception {
                String[] a = s.split(",");
                ArrayList<Integer> ret = new ArrayList<>();
                ret.add(Integer.parseInt(a[0]));
                ret.add(Integer.parseInt(a[1]));
                return ret;
            }
        }).cache();
        points.foreach(new VoidFunction<List<Integer>>() {

            @Override
            public void call(List<Integer> point) throws Exception {
                System.out.println(point.get(0) + "," + point.get(1));
            }
        });
        JavaRDD<List<Double>> kPoints = sc.textFile("E:/data/kmeans/center.txt").map(new Function<String, List<Double>>() {
            @Override
            public List<Double> call(String s) throws Exception {
                String[] a = s.split(",");
                ArrayList<Double> ret = new ArrayList<>();
                ret.add(Double.parseDouble(a[0]));
                ret.add(Double.parseDouble(a[1]));
                return ret;
            }
        });
        List<List<Double>> kPoints2 = kPoints.collect();
        for (int iter = 0; iter < iterateNum; iter++) {
            final List<List<Double>> kPoints3 = new ArrayList<>(kPoints2);
            JavaPairRDD<Integer, Pair<List<Integer>, Integer>> closet = points.mapToPair(new PairFunction<List<Integer>, Integer, Pair<List<Integer>, Integer>>() {
                @Override
                public Tuple2<Integer, Pair<List<Integer>, Integer>> call(List<Integer> point) throws Exception {
                    return new Tuple2<>(closestPoint(point, kPoints3), new Pair<>(point, 1));
                }
            });
            JavaPairRDD<Integer, Pair<List<Integer>, Integer>> newPoints = closet.reduceByKey(new Function2<Pair<List<Integer>, Integer>, Pair<List<Integer>, Integer>, Pair<List<Integer>, Integer>>() {
                @Override
                public Pair<List<Integer>, Integer> call(Pair<List<Integer>, Integer> t1, Pair<List<Integer>, Integer> t2) throws Exception {
                    return new Pair<>(addPoints(t1.getKey(), t2.getKey()), t1.getValue() + t2.getValue());
                }
            });
            JavaRDD<List<Double>> newPoints2 = newPoints.map(new Function<Tuple2<Integer, Pair<List<Integer>, Integer>>, List<Double>>() {
                @Override
                public List<Double> call(Tuple2<Integer, Pair<List<Integer>, Integer>> t) throws Exception {
                    Integer n = t._2().getValue();
                    List<Integer> point = t._2().getKey();
                    ArrayList<Double> newPoint = new ArrayList<>();
                    for (int i = 0; i < point.size(); i++) {
                        newPoint.add(0.0);
                    }
                    for (int i = 0; i < point.size(); i++) {
                        newPoint.set(i, newPoint.get(i) + point.get(i).doubleValue() / n);
                    }
                    return newPoint;
                }
            });
            List<List<Double>> newPoints3 = newPoints2.collect();
            kPoints2 = new ArrayList<>(newPoints3);

            if (iter == iterateNum - 1) {
                closet.foreach(new VoidFunction<Tuple2<Integer, Pair<List<Integer>, Integer>>>() {
                    @Override
                    public void call(Tuple2<Integer, Pair<List<Integer>, Integer>> t) throws Exception {
                        Integer index = t._1();
                        List<Integer> point = t._2().getKey();
                        Integer t2 = t._2().getValue();
                        System.out.print(index + " ");
                        System.out.print(point.get(0) + "," + point.get(1) + " ");
                        System.out.println(t2);
                    }
                });
            }
        }
        sc.stop();
    }


    public static void main(String[] args) {
        run();
    }
}
