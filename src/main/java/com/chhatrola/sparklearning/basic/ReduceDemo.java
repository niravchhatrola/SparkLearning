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
public class ReduceDemo {

    public static void main(String[] args) {

        List<Double> inputData = new ArrayList<>();
        inputData.add(10.21);
        inputData.add(210.21);
        inputData.add(160.51);
        inputData.add(21410.2);
        inputData.add(100.26);
        inputData.add(710.321);


        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("ReduceDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> myRDD = sc.parallelize(inputData);

        Double reduce = myRDD.reduce((value1, value2) -> value1 + value2);

        System.out.println("------>"+reduce);

        sc.close();


    }
}
