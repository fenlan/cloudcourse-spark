package j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.HadoopRDD;
import scala.Tuple2;
import scala.Tuple4;

/**
 * Author: cwz
 * Time: 2017/9/20
 * Description: 统计车辆的相遇次数, 输出结果到MeetCount表中，行健为eid，列族为info，列名为eid
 */
public class MeetCount {
    static String columnFamilyName = "info";
    static String inputTableName = "Record";
    static String outputTableName = "MeetCount";

    public static void main(String[] args) {
        if (HbaseUtils.createTable(outputTableName, new String[]{columnFamilyName})) {
            JavaSparkContext sc = SC.getLocalSC("MeetCount");
            Configuration inputHbaseConf = HbaseConf.getConf();
            inputHbaseConf.set(TableInputFormat.INPUT_TABLE, inputTableName);

            Configuration outputHbaseConf = HbaseConf.getConf();
            JobConf jobConf = new JobConf(outputHbaseConf);
            jobConf.setOutputFormat(TableOutputFormat.class);
            jobConf.set(TableOutputFormat.OUTPUT_TABLE, outputTableName);

            JavaPairRDD<String, Tuple2<String, String>> pairRDD1;
            JavaPairRDD<String, Tuple2<String, String>> pairRDD2;
            pairRDD1 = sc.newAPIHadoopRDD(inputHbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                    .mapToPair((Tuple2<ImmutableBytesWritable, Result> input) -> {
                                String row = Bytes.toString(input._2.getRow());
                                String placeId = row.split("##")[0];
                                String time = row.split("##")[1];
                                String eid = row.split("##")[2];
                                return new Tuple2<String, Tuple2<String, String>>(placeId, new Tuple2<>(eid, time));
                            }
                    );
            pairRDD2 = pairRDD1;
            JavaPairRDD<String, Tuple2<Tuple2<String, String>,Tuple2<String, String>>> joinPair = pairRDD1.join(pairRDD2);
            joinPair.filter((Tuple2<String, Tuple2<Tuple2<String, String>, Tuple2<String, String>>> input) -> {
                String eid1 = input._2._1._1;
                String eid2 = input._2._2._1;
                Long time1 = Long.parseLong(input._2._1._2);
                Long time2 = Long.parseLong(input._2._2._2);

                if (!eid1.equals(eid2) && Math.abs(time1 - time2) < 60)     return true;
                else                                                        return false;
            })
            .mapToPair((Tuple2<String, Tuple2<Tuple2<String, String>,Tuple2<String, String>>> input) -> {
                String eid1 = input._2._1._1;
                String eid2 = input._2._2._1;
                return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<>(eid1, eid2), 1);
            }).reduceByKey((a, b) -> a + b)
            .mapToPair((Tuple2<Tuple2<String, String>, Integer> input) -> {
                String eid1 = input._1._1;
                String eid2 = input._1._2;
                String count = Integer.toString(input._2);
                System.out.println("eid1: " + eid1 + " eid2: " + eid2 + "count: " + count);
                Put put = new Put(Bytes.toBytes(eid1));
                put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(eid2), Bytes.toBytes(count));
                return new Tuple2<>(new ImmutableBytesWritable(), put);
            }).saveAsHadoopDataset(jobConf);
            sc.stop();
        }
    }
}
