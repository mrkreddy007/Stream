package JavaStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import org.apache.hadoop.io.*;
import java.util.*;

import java.io.IOException;

/**
 * Created by ramakrishna on 1/16/17.
 */
public class Top10 {
    public static void main(String[] args) throws IOException {
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

        JavaPairRDD<Text,IntWritable> lines = ctx.sequenceFile(inputpath,Text.class,IntWritable.class,1);

        // Step4: create key value paris
        JavaPairRDD<String,Integer> pairs = lines.mapToPair(new PairFunction<Tuple2<Text,IntWritable>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Text,IntWritable>  s) {
                return new Tuple2<String, Integer>(s._1.toString(),s._2.get());
            }
        });

        List<Tuple2<String, Integer>> debug1 = pairs.collect();
        for (Tuple2<String, Integer> t2 : debug1) {
            System.out.println("key " + t2._1 + "Value " + t2._2);
        }

        //Step 5:- Create Local top-10
        JavaRDD<SortedMap<Integer, String>> partitions = pairs.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, SortedMap<Integer, String>>() {
            @Override
            public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> iter) throws Exception {
                SortedMap<Integer, String> top10 = new TreeMap<>();
                while (iter.hasNext()) {
                    Tuple2<String, Integer> tuple = iter.next();
                    top10.put(tuple._2, tuple._1);
                    if (top10.size() > 10) {
                        top10.remove(top10.firstKey());
                    }
                }
                return Collections.singletonList(top10).iterator();
            }
        });

        //Step 6 :- Find Final top 10 from all partitions

        SortedMap<Integer, String> finaltop10 = new TreeMap<>();
        List<SortedMap<Integer, String>> alltop10 = partitions.collect();

        for (SortedMap<Integer, String> localtop10 : alltop10) {
            for (Map.Entry<Integer, String> entry : localtop10.entrySet()) {
                finaltop10.put(entry.getKey(), entry.getValue());
                if (finaltop10.size() > 10) {
                    finaltop10.remove(finaltop10.firstKey());
                }
            }
        }

        // Step 7 emit Final top -10
        for (Map.Entry<Integer, String> entry : finaltop10.entrySet()) {
            System.out.println(entry.getKey() + "--" + entry.getValue());
        }
        System.exit(0);
    }
}
