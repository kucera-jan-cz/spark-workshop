package sw.rdds

import org.apache.spark.rdd._
import org.apache.spark.{Partition, TaskContext}

class CensorshipRDD(parent: RDD[String], censorTokens: String*)
  extends RDD[String](parent) {


  override def getPartitions: Array[Partition] = {
    firstParent[String].partitions
  }

  val censorPair = censorTokens.map(c => (c, c.replaceAll(".", "*")))

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    firstParent[String].iterator(split, context).map(t => {
      var censored = t
      for {(censor, replace) <- censorPair} {
        censored = censored.replaceAll(censor, replace)
      }
      censored
    })
  }

  def collectLegal(): Seq[String] = {
    val results = sparkContext.runJob(this, (it: Iterator[String]) => it.filter(l => !l.contains("*")).toArray)
    Array.concat(results: _*)
  }

}
