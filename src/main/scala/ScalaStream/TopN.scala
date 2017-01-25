package ScalaStream

import org.apache.hadoop.io._

import scala.collection.SortedMap

/**
  * Created by ramakrishna on 1/16/17.
  */
object TopN {

  def main(args: Array[String]): Unit = {
    if (args.size < 2) {
      println("Please give correct input")
      System.exit(1)

    }

    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf()
      .setAppName("Top N")
      .setMaster("local")
    val path = args(0)
    val sc = new SparkContext(conf)
    val N = sc.broadcast(args(1))
    val file = sc.sequenceFile(path, classOf[Text], classOf[IntWritable], 2)
    val pairs = file.map {
      case (x, y) => (y.get(), x.toString)
    }
    pairs.foreach(println)
    val  partitionsoutput = pairs.groupByKey().sortByKey(false).mapPartitions(itr => {
      itr.take(N.value.toInt)
    })

    val alltop10=partitionsoutput.collect()
    val finaltop10=SortedMap.empty[Int,Array[String]].++:(alltop10)
    val output=finaltop10.takeRight(N.value.toInt)
    output.foreach{
      case(k,v)=> println(s"$k \t ${v.asInstanceOf[Seq[String]].mkString(",")}")
    }

    sc.stop()
  }
}
