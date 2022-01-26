import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class WriteToHBase {

    public static void main(String[] args) throws IOException {
//        String file_name = "E:/data/test_03.csv";
        String file_name = args[0];
        SparkConf conf = new SparkConf().setAppName("WriteToHBase").setMaster("local[4]").set("spark.hadoop.validateOutputSpecs", "false");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> a = sc.textFile(file_name);
        JavaRDD<String[]> b = a.map(new Function<String, String[]>() {
            @Override
            public String[] call(String s) throws Exception {
                String[] list = s.split(",");
                for (String t : list) {
                    int len = t.length();
                    t = t.substring(1, len - 1);
                }
                return list;
            }
        });
        JavaPairRDD<ImmutableBytesWritable, Put> c = b.mapToPair(new PairFunction<String[], ImmutableBytesWritable, Put>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(String[] list) throws Exception {
                String id = list[0] + "_" + list[2];
                Put put = new Put(Bytes.toBytes(id));
                put.addColumn(Bytes.toBytes("spatio_temporal"), Bytes.toBytes("longitude"), Bytes.toBytes(list[4]));
                put.addColumn(Bytes.toBytes("spatio_temporal"), Bytes.toBytes("latitude"), Bytes.toBytes(list[3]));
                put.addColumn(Bytes.toBytes("spatio_temporal"), Bytes.toBytes("timestamp"), Bytes.toBytes(list[2]));
                put.addColumn(Bytes.toBytes("attribute"), Bytes.toBytes("speed"), Bytes.toBytes(list[5]));
                put.addColumn(Bytes.toBytes("others"), Bytes.toBytes("plan_no"), Bytes.toBytes(list[0]));
                put.addColumn(Bytes.toBytes("others"), Bytes.toBytes("truck_no"), Bytes.toBytes(list[1]));
                return new Tuple2<>(new ImmutableBytesWritable(), put);
            }
        });
        sc.hadoopConfiguration().set("hbase.zookeeper.quorum", "localhost");
        sc.hadoopConfiguration().set("hbase.zookeeper.property.clientPort", "2181");
        sc.hadoopConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "trajectory");
        Job job = new Job(sc.hadoopConfiguration());
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Result.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        c.saveAsNewAPIHadoopDataset(job.getConfiguration());
//        a.take(2).forEach(new Consumer<String>() {
//            @Override
//            public void accept(String s) {
//                System.out.println(s);
//            }
//        });
        b.take(2).forEach(new Consumer<String[]>() {
            @Override
            public void accept(String[] strings) {
                System.out.println(Arrays.toString(strings));
            }
        });
    }
}
