package JavaStream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * Created by ramakrishna on 1/16/17.
 */
public class Top10Lambda {
    public static void main(String[] args){
        //Step1: Handling Input Params
        if (args.length < 1) {
            System.err.println("not enough arguments");
            System.exit(1);
        }
        String inputpath = args[0];
        System.out.println("Input path " + inputpath);

        //Step2:- Create an instance of spark Configuration
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("Finding top N elements of key value pairs");

        //Step3:- Create Spark Context
        JavaSparkContext ctx = new JavaSparkContext(conf);

        JavaPairRDD<Text,IntWritable> lines = ctx.sequenceFile(inputpath, Text.class, IntWritable.class, 1);
        // Step4: create key value paris
        JavaPairRDD<String,Integer> pairs=lines.mapToPair(w -> new Tuple2<String, Integer>(w._1.toString(),w._2.get()));
        // Testing the values
        pairs.foreach(data ->System.out.println(data._1+ "  "+data._2));

        // Step 5- Create Local Top 10
        JavaRDD<SortedMap<Integer,String>> partitions = pairs.mapPartitions((Iterator<Tuple2<String,Integer>> iter) -> {
            SortedMap<Integer,String> top10 = new TreeMap<>();
            while (iter.hasNext())
            {
                Tuple2<String,Integer> tuple = iter.next();
                top10.put(tuple._2,tuple._1);
                if (top10.size() > 10) {
                    top10.remove(top10.firstKey());
                }
            }
            return Collections.singletonList(top10).iterator();
        });

        // Step6 -Return Final top 10
        SortedMap<Integer,String> finaltop10 = new TreeMap<>();
        List<SortedMap<Integer,String>> alltop10 = partitions.collect();
        for(SortedMap<Integer,String> localtop10 : alltop10){
            for(Map.Entry<Integer,String> entry:localtop10.entrySet()){
                finaltop10.put(entry.getKey(),entry.getValue());
                if(finaltop10.size() >10){
                    finaltop10.remove(finaltop10.firstKey());
                }
            }
        }

        for(Map.Entry<Integer,String> entry : finaltop10.entrySet()){
            System.out.println(entry.getKey()+"--"+entry.getValue());
        }
    }
}
