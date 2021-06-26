package com.chhatrola.sparklearning.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by niv214 on 26/6/21.
 */
public class RightJoinDemo {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<Tuple2<Integer, String>> userData = new ArrayList<>();
        userData.add(new Tuple2<>(1,"Nirav"));
        userData.add(new Tuple2<>(2,"Sneha"));
        userData.add(new Tuple2<>(3,"Hardik"));
        userData.add(new Tuple2<>(4, "Reena"));

        List<Tuple2<Integer, Integer>> userVisit = new ArrayList<>();
        userVisit.add(new Tuple2<>(1,214));
        userVisit.add(new Tuple2<>(2,2));
        userVisit.add(new Tuple2<>(3,74310));
        userVisit.add(new Tuple2<>(5,5));
        userVisit.add(new Tuple2<>(8,10));


        SparkConf sparkConf = new SparkConf().setAppName("RightJoinDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<Integer, String> userDataPairRdd = sc.parallelizePairs(userData);
        JavaPairRDD<Integer, Integer> userVisitPairRdd = sc.parallelizePairs(userVisit);

        JavaPairRDD<Integer, Tuple2<Optional<String>, Integer>> userDataVisitJoinRdd = userDataPairRdd.rightOuterJoin(userVisitPairRdd);


        userDataVisitJoinRdd.collect().forEach(System.out::println);

        sc.close();
    }
}
