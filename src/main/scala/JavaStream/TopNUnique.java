package JavaStream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

/**
 * Created by ramakrishna on 1/17/17.
 */
public class TopNUnique {
    public static void main(String[] args){

        // Step1:- handling Input Parameters
        if(args.length <2 ){
            System.out.println("Incorrect numbre of inputs");
            System.exit(1);
        }
         String inputpath =args[0];
        Integer broadcastN = Integer.parseInt(args[1]);
        //Step2:- Creating Spark Configurations
        SparkConf conf = new SparkConf();
        conf.setAppName("Finiding the top N Unique " +
                "elements from a given set of key-value pairs");
        conf.setMaster("local");

        JavaSparkContext ctx = new JavaSparkContext(conf);

        // Step 3:-  Broadcast the argument value of N to all nodes
        final Broadcast<Integer> topN = ctx.broadcast(broadcastN);

        //Step4:- read data from input file in hdfs or anywhere
        // Since the file is Sequence File lets use
        // Sequence file input formats.
        //Using coalesce to reduce the number of partitions
        JavaPairRDD<Text,IntWritable> inputRDD=ctx.sequenceFile(inputpath,Text.class,IntWritable.class,1).coalesce(9);

        //Step5 create Key values paris.
        JavaPairRDD<String,Integer> kv=inputRDD.mapToPair(w -> new Tuple2<String, Integer>(w._1.toString(),w._2.get()));
        // Testing the values
      //  kv.foreach(data ->System.out.println(data._1+ "  "+data._2));

        //Step6 reduce redundant keys
        JavaPairRDD<String,Integer> uniquekeys= kv.reduceByKey((Integer i1,Integer i2)-> i1+i2);
        // Testing the values
       // uniquekeys.foreach(data ->System.out.println(data._1+ "  "+data._2));
        //uniquekeys.count();
        //Step7 creating local top N values for each partition
        JavaRDD<SortedMap<Integer,String>> partitions =uniquekeys.mapPartitions((Iterator<Tuple2<String,Integer>> iter) ->{
            final int N =topN.value();
            SortedMap<Integer,String>  localtopN = new TreeMap<>();
            while(iter.hasNext()){
                Tuple2<String,Integer> currenttuple = iter.next();
                localtopN.put(currenttuple._2,currenttuple._1);
                // keep only top N
                if(localtopN.size() > N){
                    localtopN.remove(localtopN.firstKey());
                }
            }
            return Collections.singletonList(localtopN).iterator();
        });
        // Testing the values
       // partitions.foreach(data ->System.out.println(data));


        //Step 8 Finding final top N from all the partitions

         SortedMap<Integer,String> finalTopN = new TreeMap<>();
         List<SortedMap<Integer,String>> allpartitiontopN=partitions.collect();
         for (SortedMap<Integer,String> localtopN :allpartitiontopN){
             for(Map.Entry<Integer,String>  entry: localtopN.entrySet()){
                 finalTopN.put(entry.getKey(),entry.getValue());

                 // Keep only top N
                 if(finalTopN.size() >topN.getValue()){
                     finalTopN.remove(finalTopN.firstKey());
                 }
             }
         }
        //Step 9 Get output
        for(Map.Entry<Integer,String> entry : finalTopN.entrySet()){
             System.out.println(entry.getKey()+"--"+entry.getValue());
        }


    }
}
