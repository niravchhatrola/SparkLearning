package com.chhatrola.sparklearning.flatmapandfilter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by niv214 on 13/6/21.
 */
public class FilterErrorDemo {

    public static void main(String[] args) {


        Logger.getLogger("org.apache").setLevel(Level.WARN);


        List<String> logs = new ArrayList<>();
        logs.add("WARN: ABDF 4");
        logs.add("WARN: SDG 8");
        logs.add("ERROR: FDGST 6");
        logs.add("FATAL: SDFASD 5");
        logs.add("WARN: SDFS 9");
        logs.add("ERROR: DFGD 2");
        logs.add("ERROR: SGA 9");
        logs.add("FATAL: ADFG 4");
        logs.add("WARN: SDFGA 8");
        logs.add("ERROR: DFGH 0");
        logs.add("WARN: FGARG 4");

        SparkConf conf = new SparkConf().setAppName("FlatMapAndFilterDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputData = sc.parallelize(logs);
        JavaRDD<String> filterRdd = inputData.filter(val -> val.split(" ")[0].equals("ERROR:"));
        filterRdd.collect().forEach(System.out::println);


        sc.close();

    }
}
