import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.function.Consumer;

public class ReadHBase {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        String tableName = "trajectory";
        String FAMILY = "spatio_temporal";
        String COLUM_NAME = "longitude";
        String COLUM_AGE = "latitude";

        SparkConf sparkConf = new SparkConf().setAppName("SparkDataFromHbase");//.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Hbase配置
        Configuration hconf = HBaseConfiguration.create(configuration);// kerberos认证集群必须传递已经认证过的conf
        hconf.set("hbase.zookeeper.quorum", "192.168.50.101");
        hconf.set("hbase.zookeeper.property.clientPort", "2181");
        hconf.set(TableInputFormat.INPUT_TABLE, tableName);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_AGE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_NAME));
        try {
            //添加scan
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            hconf.set(TableInputFormat.SCAN, ScanToString);

            //读HBase数据转化成RDD
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(hconf,
                    TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            hbaseRDD.cache();// 对myRDD进行缓存
            System.out.println("数据总条数：" + hbaseRDD.count());

            //将Hbase数据转换成PairRDD，年龄：姓名
            JavaPairRDD<Integer, String> mapToPair = hbaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable,
                    Result>, Integer, String>() {
                private static final long serialVersionUID = -2437063503351644147L;

                @Override
                public Tuple2<Integer, String> call(
                        Tuple2<ImmutableBytesWritable, Result> resultTuple2) throws Exception {
                    byte[] o1 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_NAME));//取列的值
                    byte[] o2 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_AGE));//取列的值
                    return new Tuple2<Integer, String>(new Integer(new String(o2)), new String(o1));
                }
            });
            mapToPair.take(5).forEach(new Consumer<Tuple2<Integer, String>>() {
                @Override
                public void accept(Tuple2<Integer, String> t) {
                    System.out.println("longitude: " + t._1() + ", latitude: " + t._2());
                }
            });
            //按年龄降序排序
//            JavaPairRDD<Integer, String> sortByKey = mapToPair.sortByKey(false);
            //写入数据到hdfs系统
//            sortByKey.saveAsTextFile("hdfs://localhost:8020/tmp/test");

            hbaseRDD.unpersist();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("over");
        }
    }
}
