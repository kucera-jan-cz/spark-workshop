package local

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

class RDDTest extends FunSuite with SharedSparkContext {
  val logger = LoggerFactory.getLogger(classOf[RDDTest])

  test("All lines from trailers") {
    val replaced = replaceNames()
    val trailerOne = replaced.sample(false, 0.1)
    val trailerTwo = replaced.sample(false, 0.1)
    val allTrailers = trailerOne ++ trailerTwo
    allTrailers.collect().foreach(println)
  }

  test("All possible dialog lines between Leia and Jabba") {
    val leia = "Princess Leia"
    val jabba = "Jabba the Hut"
    val replaced = replaceNames()
    val leiaLines = replaced.filter(l => l.startsWith(leia))
    val jabbaLines = replaced.filter(l => l.startsWith(jabba))
    val dialogs = leiaLines.cartesian(jabbaLines)
    //    dialogs.foreach(println)
    val firstLine = dialogs.first()
    println("Dialog:")
    println(s"${firstLine._1}")
    println(s"${firstLine._2}")
  }

  private def replaceNames(): RDD[String] = {
    val leia = "Princess Leia"
    val jabba = "Jabba the Hut"
    val all = sc.textFile("src/main/resources/all-shakespeare.txt")
    val romeoAndJuliet = all.filter(l => l.startsWith("JULIET") || l.startsWith("ROMEO"))
    val filtered = romeoAndJuliet.filter(l => !l.matches("""\s*ROMEO AND JULIET\s*"""))
    val replaced = filtered.map(l => l.replaceAll("ROMEO", jabba)).map(l => l.replaceAll("JULIET", leia))
    replaced
  }
}
