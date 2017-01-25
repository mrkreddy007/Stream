package ScalaStream

/**
  * Created by ramakrishna on 1/12/17.
  */
object AverageOfKeyValuePairs {
  import org.apache.spark.{SparkContext,SparkConf}
  import scala.Tuple1
  def main(args: Array[String]) : Unit= {
    val conf = new SparkConf()
      .setAppName("Average of Key value pairs")
      .setMaster("local")
    val sc = new SparkContext(conf)
    type ScoreCollector = (Int,Double)
    type PersonScores = (String, ( Int,Double))

    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val input = sc.parallelize(initialScores).cache()

    val cb= (score: Double) => (1,score)



    val combiner = (collector: ScoreCollector,score:Double) => {
      val (counts, totalScore) = collector
      (counts + 1,totalScore+score)
    }
    val merger = (collector1: ScoreCollector, collector2: ScoreCollector) => {
      val (counts1,totalScore1) = collector1
      val ( counts2,totalScore2) = collector2
      (counts1 + counts2,totalScore1 + totalScore2)
    }

    val averages = input.combineByKey(cb,combiner, merger)

    val avergaingFucntion = (personScore: PersonScores) => {
      val (name, (totalScore, numberScores)) = personScore
      (name, totalScore / numberScores)
    }
      val avgscores=averages.collectAsMap()
      val averageScores = averages.collectAsMap().map(avergaingFucntion)
    averageScores.foreach(println)
  }
}
