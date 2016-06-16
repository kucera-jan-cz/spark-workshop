package local

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import sw.rdds.CensorshipRDD

class CensorshipRDDTest extends FunSuite with SharedSparkContext {
  test("censoring") {
    val text = sc.parallelize(List("something", "Hadoop rocks!", "Scala else"))
    val censored = new CensorshipRDD(text, "Hadoop", "Scala")
    censored.collect().foreach(println)
  }

  test("collect legal") {
    val text = sc.parallelize(List("something", "Hadoop rocks!", "Scala else"))
    val censored = new CensorshipRDD(text, "Hadoop", "Scala")
    censored.collectLegal().foreach(println)
  }
}
