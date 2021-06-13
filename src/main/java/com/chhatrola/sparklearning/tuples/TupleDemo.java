package com.chhatrola.sparklearning.tuples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by niv214 on 13/6/21.
 */
public class TupleDemo {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<Integer> inputs = new ArrayList<>();
        inputs.add(1);
        inputs.add(4);
        inputs.add(9);
        inputs.add(16);
        inputs.add(25);
        inputs.add(36);


        SparkConf conf = new SparkConf().setAppName("TupleDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> inputRdd = sc.parallelize(inputs);

        JavaRDD<Tuple2<Integer, Double>>  valSqrtValRdd =inputRdd.map(val -> new Tuple2(val, Math.sqrt(val)));

        valSqrtValRdd.collect().forEach(System.out::println);

        sc.close();


    }

}
