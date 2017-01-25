package JavaStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by ramakrishna on 1/10/17.
 */
public class AverageTest {
    public  static void main(String[] args) {
        // TODO Auto-generated method stub


        System.out.println("Creating Spark Config");

        SparkConf JavaConf = new SparkConf();
        JavaConf.setMaster("local");
        JavaConf.setAppName("My Fisrt Java Application");

        System.out.println("Creating Spark Context");
        JavaSparkContext jspc = new JavaSparkContext(JavaConf);
        jspc.setLogLevel("WARN");
        JavaRDD<Integer> rdd = jspc.parallelize(Arrays.asList(1, 2, 3, 4));


        Average initial = new Average(0, 0);

        Average result = rdd.aggregate(initial, initial.addcount,initial.combine);
        System.out.println(result.avg());


    }


}
