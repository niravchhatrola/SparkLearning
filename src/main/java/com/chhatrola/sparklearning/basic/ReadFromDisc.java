package com.chhatrola.sparklearning.basic;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by niv214 on 13/6/21.
 */
public class ReadFromDisc {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("FlatMapAndFilterDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> stringJavaRDD = sc.textFile("src/main/resources/biglog.txt");

        stringJavaRDD.collect().forEach(System.out::println);

        sc.close();

    }
}
