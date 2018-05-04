package br.com.rivendel.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Main {

    def main(args: Array[String]): Unit = {

        val file = args(0)
        val outputDir = args(1)

        Logger.getLogger("org").setLevel(Level.ERROR)
        val conf = new SparkConf()
            .setAppName("WordCount")

        val sc = new SparkContext(conf)

        val lines = sc.textFile(file)
        val words = lines.flatMap(line => line.split(" "))

        val wordCounts = words.countByValue()

        val rdd = sc.parallelize(wordCounts.toSeq)
        rdd.saveAsTextFile(outputDir)
    }

}
