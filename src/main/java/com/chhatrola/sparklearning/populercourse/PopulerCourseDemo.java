package com.chhatrola.sparklearning.populercourse;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by niv214 on 26/6/21.
 */
public class PopulerCourseDemo {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf().setAppName("PopulerCourseDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> chapterUserPairRDD = sc.textFile("src/main/resources/UserChapterMapping");
        JavaPairRDD<Integer, Integer> chapterUserDataPairRdd = chapterUserPairRDD.mapToPair(data -> new Tuple2<>(Integer.parseInt(data.split(" ")[1]), Integer.parseInt(data.split(" ")[0])));

        JavaRDD<String> courseChapterPairRDD = sc.textFile("src/main/resources/CourseCheptherMapping");
        JavaPairRDD<Integer, Integer> courseChapterDataPairRDD = courseChapterPairRDD.mapToPair(data -> new Tuple2<>(Integer.parseInt(data.split(" ")[0]), Integer.parseInt(data.split(" ")[1])));

        JavaPairRDD<Integer, Float> courseChapterWaitagePairRDD = courseChapterDataPairRDD.mapToPair(data -> new Tuple2<>(data._1, 1))
                .reduceByKey((val1, val2) -> val1 + val2)
                .mapToPair(data -> new Tuple2<>(data._1, (float)(100/data._2)));

        JavaPairRDD<Integer, Tuple2<Integer, Float>> chapterCourseWaitagePairRdd = courseChapterDataPairRDD.join(courseChapterWaitagePairRDD)
                .mapToPair(data -> new Tuple2<>(data._2._1, new Tuple2<>(data._1, data._2._2)));


        JavaPairRDD<Integer, Tuple2<Integer, Tuple2<Integer, Float>>> chapterUserCourseWaitage = chapterUserDataPairRdd.join(chapterCourseWaitagePairRdd);

        JavaPairRDD<Integer, Float> courseWaitagePairRdd = chapterUserCourseWaitage.mapToPair(data -> new Tuple2<>(new Tuple2<>(data._1, data._2._1), data._2._2))
                .groupByKey().mapToPair(data -> new Tuple2<>(data._1, data._2.iterator().next())).mapToPair(data -> data._2);

        JavaRDD<Integer> courseWaitTotalRdd = courseWaitagePairRdd.reduceByKey((val1, val2) -> val1 + val2).mapToPair(data -> new Tuple2<>(data._2, data._1)).sortByKey(false).map(data -> data._2);

        courseWaitTotalRdd.collect().forEach(System.out::println);


        sc.close();
    }

}
