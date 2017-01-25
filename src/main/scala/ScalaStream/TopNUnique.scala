package ScalaStream

import org.apache.hadoop.io.{IntWritable, Text}

import scala.collection.SortedMap

/**
  * Created by ramakrishna on 1/17/17.
  */
object TopNUnique {
  def main(args: Array[String]): Unit = {
    if (args.size < 2) {
      println("Please give correct input")
      System.exit(1)

    }

    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf()
      .setAppName("Top N Uniqeu")
      .setMaster("local")
    val path = args(0)
    val sc = new SparkContext(conf)
    val N = sc.broadcast(args(1))
    val file = sc.sequenceFile(path, classOf[Text], classOf[IntWritable], 5)
    val kv =file.map{
      case (x,y) => (y.get(),x.toString)}
    val uniquekeys=kv.reduceByKey(_+_).map(_.swap)
    val createCombiner = (v: Int) => v
    val mergeValue = (a: Int, b: Int) => (a + b)
    val  partitionsoutput = uniquekeys.combineByKey(createCombiner, mergeValue, mergeValue)
      .groupByKey()
      .sortByKey(false)
        .mapPartitions(itr => {
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
