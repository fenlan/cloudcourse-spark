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
import scala.Tuple3;
import scala.Tuple4;

/**
 * Author: cwz
 * Time: 2017/9/20
 * Description: 统计车辆的相遇次数, 输出结果到MeetCount表中，行健为eid，列族为info，列名为eid
 */
public class Trace {
    static String columnFamilyName = "info";
    static String inputTableName = "Record";
    static String outputTableName = "Trace";

    public static void main(String[] args) {
        if (HbaseUtils.createTable(outputTableName, new String[]{columnFamilyName})) {
            JavaSparkContext sc = SC.getLocalSC("Trace");
            Configuration inputHbaseConf = HbaseConf.getConf();
            inputHbaseConf.set(TableInputFormat.INPUT_TABLE, inputTableName);

            Configuration outputHbaseConf = HbaseConf.getConf();
            JobConf jobConf = new JobConf(outputHbaseConf);
            jobConf.setOutputFormat(TableOutputFormat.class);
            jobConf.set(TableOutputFormat.OUTPUT_TABLE, outputTableName);

            sc.newAPIHadoopRDD(inputHbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                    .mapToPair((Tuple2<ImmutableBytesWritable, Result> input) -> {
                                String row = Bytes.toString(input._2.getRow());
                                String placeId = row.split("##")[0];
                                String time = row.split("##")[1];
                                String eid = row.split("##")[2];
                                String address = Bytes.toString(input._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("address")));
                                String latitude = Bytes.toString(input._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("latitude")));
                                String longitude = Bytes.toString(input._2.getValue(Bytes.toBytes(columnFamilyName), Bytes.toBytes("longitude")));
                                return new Tuple2<String, Tuple3<String, String, String>>(eid, new Tuple3<>(time, longitude, latitude));
                            }
                    )
                    .mapToPair((Tuple2<String, Tuple3<String, String, String>> input) -> {
                        String eid = input._2._1();
                        String time = input._1;
                        String longitude = input._2._2();
                        String latitude = input._2._3();
                        return new Tuple2<String, Tuple3<String, String, String>>(time, new Tuple3<>(eid, longitude, latitude));
                    })
                    .mapToPair((Tuple2<String, Tuple3<String, String, String>> input) -> {
                        String time = input._1;
                        String eid = input._2._1();
                        String longitude = input._2._2();
                        String latitude = input._2._3();
                        System.out.println("eid: " + eid + " time: " + time + " longitude: " + longitude + " latitude: " + latitude);
                        Put put = new Put(Bytes.toBytes(eid));
                        put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(time), Bytes.toBytes(longitude + "#" + latitude));
                        return new Tuple2<>(new ImmutableBytesWritable(), put);
                    }).saveAsHadoopDataset(jobConf);
            sc.stop();
        }
    }
}
