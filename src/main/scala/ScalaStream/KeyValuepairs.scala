package ScalaStream

/**
  * Created by ramakrishna on 1/11/17.
  */
object KeyValuepairs {


  import org.apache.spark.{SparkConf, SparkContext}
  import scala.Tuple1;


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Average")
      .setMaster("local")
    val sc = new SparkContext(conf)

    type ScoreCollector=(Int,Double)
    type PersonScores=(String ,(Int,Double))
   val initialScores = Array(("Fred",88.0),("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))

    val wilmaAndFredScores=sc.parallelize(initialScores).cache()

    val createScoreCombiner = (score :Double) => (1,score)

    val scoreCombiner = (collector: ScoreCollector, score: Double) => {
      val (numberScores, totalScore) = collector
      (numberScores + 1, totalScore + score)
    }
    val scoreMerger =(collector1:ScoreCollector,collector2:ScoreCollector) => {
      val (numberScores1,totalScore1)=collector1
      val (numberScores2,totalScore2)=collector2
      (numberScores1+numberScores2,totalScore1+totalScore2)
    }
    val scores =wilmaAndFredScores.combineByKey(createScoreCombiner,scoreCombiner,scoreMerger)

val avergaingFucntion =(personScore : PersonScores) => {
  val (name,(numberScores,totalScore))= personScore
  (name,totalScore/numberScores)
}
    val avergaeScores = scores.collectAsMap().map(avergaingFucntion)
    val avgscores=scores.collectAsMap()
     avgscores.foreach(println)
    println("Average Scores using Combining Key")
    avergaeScores.foreach((ps) =>
    {
      val(name,average)= ps
      println(name +" 's avergae score : "+ average)
    })
    sc.stop()

  }
}