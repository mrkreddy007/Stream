package ScalaStream

/**
  * Created by ramakrishna on 1/10/17.
  */
object Average {

  import org.apache.spark.{SparkConf,SparkContext}
import scala.Tuple1;
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf()
                 .setAppName("Average")
                   .setMaster("local")
    val sc = new SparkContext(conf)

    val lst = sc.parallelize(List.range(1,10))

 val result = lst.aggregate((0,0))((acc,value) => (acc._1+value , acc._2+1),
                                    (acc1,acc2) => (acc1._1+acc2._1,acc1._2+acc2._2))
   val inter=print(result)
    val avergae=result._1/result._2
  System.out.println(avergae.toString)
  }



}
