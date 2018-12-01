package com.sakura.spark.kafka;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 使用Java开发Spark应用程序
 */
public class WordCountApp {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("WordCountApp").master("local[2]").getOrCreate();

        JavaRDD<String> lines = sparkSession.read().textFile("e:/hello.txt").javaRDD();

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("\t")).iterator());

        JavaPairRDD<String, Integer> counts = words
                .mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<String, Integer> tuple2 : output) {
            System.out.println(tuple2._1() + " : " + tuple2._2());
        }

        sparkSession.stop();
    }
}
