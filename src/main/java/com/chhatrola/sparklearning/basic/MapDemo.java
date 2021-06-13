package com.chhatrola.sparklearning.basic;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by niv214 on 12/6/21.
 */
public class MapDemo {

    public static void main(String[] args) {

        List<Double> inputData = new ArrayList<>();
        inputData.add(10.21);
        inputData.add(210.21);
        inputData.add(160.51);
        inputData.add(21410.2);
        inputData.add(100.26);
        inputData.add(710.321);


        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("MapDemo").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> myRDD = sc.parallelize(inputData);

        JavaRDD<Double> sqrtRDD = myRDD.map(value1 -> Math.sqrt(value1));

        sqrtRDD.collect().forEach(System.out::println);

        sc.close();

    }
}
