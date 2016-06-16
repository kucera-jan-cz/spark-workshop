package local

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class PairTest extends FunSuite with SharedSparkContext {
  override val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "true").
    set("spark.app.id", appID)

  test("join test") {
    def wc = sc.textFile("src/main/resources/all-shakespeare.txt")
      .flatMap(_.split("""\W+"""))
      .map(_.toLowerCase())
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .map(identity)

    def heroes = sc.textFile("src/main/resources/all-shakespeare.txt")
      .flatMap(_.split("""\W+"""))
      .map(_.toLowerCase())
      .filter(word => word == "juliet" || word == "romeo")
      .map {
        case "juliet" => ("juliet", 100)
        case "romeo" => ("romeo", 200)
      }

    val join = wc.join(heroes)

    join.collect().foreach(println)
    println("Finished")
  }

  test("sort by key") {
    val allShakespeare = sc.textFile("src/main/resources/all-shakespeare.txt")
    val pairs = allShakespeare.filter(_.trim != "").groupBy(_.charAt(0))
    pairs.sortByKey(false).take(5).map(println(_))
    pairs.sortBy({case (k, v) => v}).take(5).map(println(_))
  }

  test("word count") {
    val txt = sc.textFile("src/main/resources/all-shakespeare.txt")
    val words = txt.filter(_.trim != "").flatMap(l => l.split("\\s+").filter(w => w.size > 0)).map(w => (w, 1))
    val uniques = words.reduceByKey({case (a, t) => a + t})
    uniques.sortBy({case (w, c) => c}).foreach({case (w, c) => println(s"|$w| : $c")})
    println("Finished")
//    val uniques = words.groupByKey()
//    uniques.mapValues(i => i.sum).sortBy({case (w, c) => c}).foreach({case (w, c) => println(s"$w : $c")})

  }

  test("mid-term") {
    val romeoTxt = sc.textFile("src/main/resources/all-shakespeare.txt")
    val words = romeoTxt.filter(_.trim != "").flatMap(l => l.split("\\s+").filter(w => w.size > 0)).map(w => (w.toUpperCase, 1))
    val uniques = words.reduceByKey({case (a, t) => a + t}).filter({case (k,v) => "ROMEO".equals(k)})

    val femaleTxt = sc.textFile("src/main/resources/female-names.txt")
    val femalePair = toPair(femaleTxt)
    val maleTxt = sc.textFile("src/main/resources/male-names.txt")
    val malePair = toPair(maleTxt)
    val humanPair = malePair ++ femalePair

    val found = uniques.join(humanPair)
    found.foreach(println(_))
  }

  private def toPair(text: RDD[String]) : RDD[(String, Int)] = {
    text.map(l => {
      val splits = l.split("\\s+")
      (splits(0), splits(3).toInt)
    }).sortBy({case (k,v) => v})
  }
}
