package com.chhatrola.sparklearning.wordcount;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by niv214 on 22/6/21.
 */
public class WordCountProblem {

    static List<String> boringWords = Arrays.asList("the", "of", "or", "and", "to", "any", "in", "that",
            "you", "a", "such", "for", "by", "this", "is", "with", "as", "shall",
            "under", "not");


    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf().setAppName("WordCountProblem").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> data = sc.textFile("src/main/resources/LICENSE");
        JavaRDD<String> plainData = data.map(line -> line.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
        JavaRDD<String> wordsRDD = plainData.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Long> wordPairRdd = wordsRDD.mapToPair(word -> new Tuple2<>(word.trim(), 1L))
                                                    .filter(pairRdd -> pairRdd._1.length()>0)
                                                    .filter(word -> !boringWords.contains(word._1));
        JavaPairRDD<String, Long> wordCountPairRdd = wordPairRdd.reduceByKey((val1, val2) -> val1 + val2);

        JavaPairRDD<Long, String> countWordPairRdd = wordCountPairRdd.mapToPair(wordCountMap -> new Tuple2<>(wordCountMap._2, wordCountMap._1));

        JavaPairRDD<Long, String> sortedPairRdd = countWordPairRdd.sortByKey(false);

        sortedPairRdd.take(10).forEach(wordCount -> System.out.println(wordCount._2+" is being repeated "+wordCount._1+" times"));

        sc.close();

    }



}
