package br.com.rivendel.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Main {

    def main(args: Array[String]): Unit = {

        var text = ""

        if (args.length > 0) {
            text = args(0)
        } else {
            text = "src/main.resources/text.txt"
        }

        Logger.getLogger("org").setLevel(Level.ERROR)
        val conf = new SparkConf()
            .setAppName("WordCount")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)

        val lines = sc.textFile("src/main/resources/text.txt")
        val words = lines.flatMap(line => line.split(" "))

        val wordCounts = words.countByValue()
        for ((word, count) <- wordCounts) println(word + " : " + count)

    }

}
