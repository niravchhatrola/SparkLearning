package com.chhatrola.sparklearning.pairrdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by niv214 on 13/6/21.
 */
public class PairRddDemo {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);


        List<String> logs = new ArrayList<>();
        logs.add("WARN: ABDF");
        logs.add("WARN: SDG");
        logs.add("ERROR: FDGST");
        logs.add("FATAL: SDFASD");
        logs.add("WARN: SDFS");
        logs.add("ERROR: DFGD");
        logs.add("ERROR: SGA");
        logs.add("FATAL: ADFG");
        logs.add("WARN: SDFGA");
        logs.add("ERROR: DFGH");
        logs.add("WARN: FGARG");

        SparkConf conf = new SparkConf().setAppName("PairRddDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputData = sc.parallelize(logs);
        JavaPairRDD<String, Long> pairRdd = inputData.mapToPair(val -> {
            String[] split = val.split(":");
            String key = split[0];

            return new Tuple2<>(key, 1L);
        });

        JavaPairRDD<String, Long> wordCountPairRDD = pairRdd.reduceByKey((val1, val2) -> val1 + val2);

        wordCountPairRDD.collect().forEach(System.out::println);

        sc.close();

    }

}
