package com.acision.ain.cb.local

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

class WordcountTest extends FunSuite with SharedSparkContext {
  val logger = LoggerFactory.getLogger(classOf[WordcountTest])
  test("test count") {
    val ints = for{i <- 1 to 10000} yield i
    val rdd = sc.parallelize(ints)
    val filtered = rdd.filter(i => i % 5 == 0).filter(i => i % 2 == 0)
    logger.info("COUNT: {}", filtered.count())
  }
}
