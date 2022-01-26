import model.Grid;
import model.Point;
import model.Road;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

public class MapMatching3 {
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

    public static JavaRDD<List<Integer>> map_matching(Map<Integer, Set<Road>> mp_roads, JavaRDD<List<Point>> pois, Grid grid) {
        JavaRDD<List<Integer>> road_id_list = pois.map((Function<List<Point>, List<Integer>>) poi -> get_closest_road_id(poi, mp_roads, grid));
        System.out.println("第一个" + pois.first());
        System.out.println(pois.count());
        return road_id_list;
    }

    private static List<Integer> get_closest_road_id(List<Point> poi, Map<Integer, Set<Road>> mp_roads, Grid grid) {
        List<Integer> ret = new ArrayList<>();
        //使用网格索引筛选可能用到的路段
        List<Road> part_roads = get_part_roads(poi, mp_roads, grid);
        for (Point one_poi : poi) {
            int inx = 0;
            float dis = Float.POSITIVE_INFINITY;
            //只从网格索引筛选出来的路段中选择进行匹配
            for (Road road : part_roads) {
                float cur_dis = cal_distance(one_poi, road);
                if (cur_dis < dis) {
                    dis = cur_dis;
                    inx = road.getId();
                }
            }
            ret.add(inx);
        }
        // 去重
        HashSet<Integer> hsh = new HashSet<>(ret);
        ret.clear();
        ret.addAll(hsh);
        return ret;
    }

    private static List<Road> get_part_roads(List<Point> poi, Map<Integer, Set<Road>> mp_roads, Grid grid) {
        Set<Integer> set_id = new HashSet<>();
        for (Point p : poi) {
            set_id.add(grid.getHashId(p));
        }
        Set<Road> use_road = new HashSet<>();
        for (int id : set_id) {
//            System.out.println("135, " + id);
            if (!mp_roads.containsKey(id)) {
                continue;
            }
            use_road.addAll(mp_roads.get(id));
        }
        return new ArrayList<>(use_road);
    }

    private static Double[] get_lon_lat_range(JavaRDD<Road> roads) {
        JavaPairRDD<String, Road> pair_roads = roads.mapToPair(new PairFunction<Road, String, Road>() {
            @Override
            public Tuple2<String, Road> call(Road road) throws Exception {
                return new Tuple2<>("key", road);
            }
        });
        JavaPairRDD<String, Iterable<Road>> pair_roads2 = pair_roads.groupByKey();
        JavaRDD<Double[]> lon_lat_range = pair_roads2.map(new Function<Tuple2<String, Iterable<Road>>, Double[]>() {
            @Override
            public Double[] call(Tuple2<String, Iterable<Road>> t) throws Exception {
                //min_lon,min_lat,max_lon,max_lat
                Double[] range = {180.0, 90.0, -180.0, -90.0};
                for (Road road : t._2()) {
                    if (road.getStart_point().getLongitude() < range[0]) {
                        range[0] = Double.parseDouble(road.getStart_point().getLongitude() + "");
                    } else if (road.getStart_point().getLongitude() > range[2]) {
                        range[2] = Double.parseDouble(road.getStart_point().getLongitude() + "");
                    }
                    if (road.getStart_point().getLatitude() < range[1]) {
                        range[1] = Double.parseDouble(road.getStart_point().getLatitude() + "");
                    } else if (road.getStart_point().getLatitude() > range[3]) {
                        range[3] = Double.parseDouble(road.getStart_point().getLatitude() + "");
                    }
                    if (road.getEnd_point().getLongitude() < range[0]) {
                        range[0] = Double.parseDouble(road.getEnd_point().getLongitude() + "");
                    } else if (road.getEnd_point().getLongitude() > range[2]) {
                        range[2] = Double.parseDouble(road.getEnd_point().getLongitude() + "");
                    }
                    if (road.getEnd_point().getLatitude() < range[1]) {
                        range[1] = Double.parseDouble(road.getEnd_point().getLatitude() + "");
                    } else if (road.getEnd_point().getLatitude() > range[3]) {
                        range[3] = Double.parseDouble(road.getEnd_point().getLatitude() + "");
                    }
                }
                return range;
            }
        });
        return lon_lat_range.collect().get(0);
    }

    public static void main(String[] args) {
        String roadFile = "E:/data/jiaozhou/roads.txt";
        String poiFile = "E:/data/jiaozhou/tra.txt";
        String output = "E:/data/jiaozhou/result_04/";
//        String roadFile = args[0];
//        String poiFile = args[1];
//        String output = args[2];
        System.out.println("Hello world");
        SparkConf conf = new SparkConf().setAppName("Map_matching3").setMaster("local[6]");
//        SparkConf conf = new SparkConf().setAppName("Map_matching3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Road> roads = readOSM(sc, roadFile);
        JavaRDD<List<Point>> pois = read_trajectory(sc, poiFile);
        List<Road> new_roads = roads.collect();
        //路段网格化
        //找出roads的经纬度范围[min_lon,min_lat,max_lon,max_lat]
        Double[] range = get_lon_lat_range(roads);
        System.out.println(Arrays.toString(range));
        Grid grid = new Grid(range[0], range[2], range[1], range[3]);
        Map<Integer, Set<Road>> mp_roads = new HashMap<>();
        for (Road road : new_roads) {
            int id = grid.getHashId(road.getStart_point());
            Set<Road> t;
            if (!mp_roads.containsKey(id)) {
//                System.out.print(id + " ");
                t = new HashSet<>();
            } else {
                t = mp_roads.get(id);
            }
            t.add(road);
            mp_roads.put(id, t);

            id = grid.getHashId(road.getEnd_point());
            if (!mp_roads.containsKey(id)) {
//                System.out.print(id + " ");
                t = new HashSet<>();
            } else {
                t = mp_roads.get(id);
            }
            t.add(road);
            mp_roads.put(id, t);
        }
        JavaRDD<List<Integer>> road_id_list = map_matching(mp_roads, pois, grid);
        road_id_list.saveAsTextFile(output);
    }
}
