package hdfs

import org.apache.spark.{SparkConf, SparkContext}

object HdfsSpark extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("HdfsSparkApp")
  val sc = new SparkContext(conf)

  val rdd1 = sc.textFile("hdfs://localhost:9000/spark/loremIpsum-20")
  val rdd2 = rdd1.flatMap(x => x.split(" "))
  val rdd3 = rdd2.map(x => (x, 1))
  val rdd4 = rdd3.reduceByKey(_ + _)

  rdd4.saveAsTextFile("hdfs://localhost:9000/results/wordcount")
}

