import model.Point;
import model.Road;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class MapMatching2 {
    public static JavaRDD<Road> readOSM(JavaSparkContext sc, String file_name) {
        System.out.println("read OSM");
//        String inputFile = "E:\\data\\map_matching\\roads.txt";
//        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<String> input = sc.textFile(file_name);
        System.out.println(input.first());
        System.out.println("读入的个数" + input.count());
        JavaRDD<Road> roads = input.map((Function<String, Road>) s -> {
//                System.out.println("s的长度"+s.length());
            int id = Integer.parseInt(s.split(" ")[0]);
            float st_lon = Float.parseFloat(s.split(" ")[2]);
            float st_lat = Float.parseFloat(s.split(" ")[3]);
            float ed_lon = Float.parseFloat(s.split(" ")[4]);
            float ed_lat = Float.parseFloat(s.split(" ")[5]);
            Road road = new Road(id, new Point(st_lon, st_lat), new Point(ed_lon, ed_lat));
            return road;
        });
        System.out.println("第一个" + roads.first());
        System.out.println(roads.count());
        return roads;
    }

    public static JavaRDD<List<Point>> read_trajectory(JavaSparkContext sc, String file_name) {
        System.out.println("read trajectory");
//        String inputFile = "E:\\data\\map_matching\\tra.txt";
//        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<String> input = sc.textFile(file_name);
        System.out.println(input.first());
        System.out.println("读入的个数" + input.count());
        JavaRDD<List<Point>> pois = input.map((Function<String, List<Point>>) s -> {
//                System.out.println("s的长度"+s.length());
            String[] poi = s.split(",");
            List<Point> arr = new ArrayList<>();
            for (String value : poi) {
                float lon = Float.parseFloat(value.split(" ")[0]);
                float lat = Float.parseFloat(value.split(" ")[1]);
                arr.add(new Point(lon, lat));
            }
            return arr;
        });
        System.out.println("第一个" + pois.first());
        System.out.println(pois.count());
        return pois;
    }

    public static float cal_distance(Point poi, Road road) {
        Point x0 = wgs84_to_mercator(poi);
        Point x1 = wgs84_to_mercator(road.getStart_point());
        Point x2 = wgs84_to_mercator(road.getEnd_point());
        double a = line_space(x1, x2);
        double b = line_space(x1, x0);
        double c = line_space(x2, x0);
        double dis = 0.0;
        if (c <= 0.000001 || b <= 0.000001) {
            dis = 0.0;
        } else if (a <= 0.000001) {
            dis = b;
        } else if (c * c >= a * a + b * b) {
            dis = b;
        } else if (b * b >= a * a + c * c) {
            dis = c;
        } else {
            double p = (a + b + c) / 2;
            double s = Math.sqrt(p * (p - a) * (p - b) * (p - c));
            dis = 2 * s / a;
        }
        return (float) dis;
    }

    private static double line_space(Point x1, Point x2) {
        float dx = x1.getLatitude() - x2.getLatitude();
        float dy = x1.getLongitude() - x2.getLongitude();
        return Math.sqrt(dx * dx + dy * dy);
    }

    private static Point wgs84_to_mercator(Point poi) {
        double x = poi.getLongitude() * 20037508.342789 / 180;
        double y = Math.log(Math.tan((90 + poi.getLatitude()) * Math.PI / 360)) / (Math.PI / 180);
        y = y * 20037508.34789 / 180;
        return new Point((float) x, (float) y);
    }

    public static JavaRDD<List<Integer>> map_matching(List<Road> new_roads, JavaRDD<List<Point>> pois) {
        JavaRDD<List<Integer>> road_id_list = pois.map((Function<List<Point>, List<Integer>>) poi -> get_closest_road_id(poi, new_roads));
        System.out.println("第一个" + pois.first());
        System.out.println(pois.count());
        return road_id_list;
    }

    private static List<Integer> get_closest_road_id(List<Point> poi, List<Road> roads) {
        List<Integer> ret = new ArrayList<>();
        for (Point one_poi : poi) {
            int inx = 0;
            float dis = Float.POSITIVE_INFINITY;
            for (Road road : roads) {
                float cur_dis = cal_distance(one_poi, road);
                if (cur_dis < dis) {
                    dis = cur_dis;
                    inx = road.getId();
                }
            }
            ret.add(inx);
        }
        // 去重！！！！！！！！！！！！！！！！！！！！！！！！！！！！！
//        HashSet<Integer> hsh = new HashSet<>(ret);
//        ret.clear();
//        ret.addAll(hsh);
        return ret;
    }

    public static void main(String[] args) {
        String roadFile = "E:/data/kaixuan/roads.txt";
        String poiFile = "E:/data/kaixuan/tra_all.txt";
        String output = "E:/data/kaixuan/result00/";
//        String roadFile = args[0];
//        String poiFile = args[1];
//        String output = args[2];
        System.out.println("Hello world");
        SparkConf conf = new SparkConf().setAppName("Map_matching2").setMaster("local[6]");
//        SparkConf conf = new SparkConf().setAppName("Map_matching2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Road> roads = readOSM(sc, roadFile);
        JavaRDD<List<Point>> pois = read_trajectory(sc, poiFile);
        List<Road> new_roads = roads.collect();
        JavaRDD<List<Integer>> road_id_list = map_matching(new_roads, pois);
        road_id_list.saveAsTextFile(output);
    }
}
