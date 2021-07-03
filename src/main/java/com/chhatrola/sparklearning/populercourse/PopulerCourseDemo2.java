package com.chhatrola.sparklearning.populercourse;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Scanner;

/**
 * Created by niv214 on 26/6/21.
 */
public class PopulerCourseDemo2 {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf().setAppName("PopulerCourseDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> chapterUserPairRDD = sc.textFile("src/main/resources/UserChapterMapping");
        JavaPairRDD<Integer, Integer> chapterUserDataPairRdd = chapterUserPairRDD.mapToPair(data -> new Tuple2<>(Integer.parseInt(data.split(" ")[1]), Integer.parseInt(data.split(" ")[0])));

        JavaRDD<String> courseChapterPairRDD = sc.textFile("src/main/resources/CourseCheptherMapping");
        JavaPairRDD<Integer, Integer> chapterCourseDataPairRDD = courseChapterPairRDD.mapToPair(data -> new Tuple2<>(Integer.parseInt(data.split(" ")[1]), Integer.parseInt(data.split(" ")[0])));

        JavaPairRDD<Tuple2<Integer, Integer>, Integer> chapterUserCourse = chapterUserDataPairRdd.join(chapterCourseDataPairRDD)
                .mapToPair(data -> new Tuple2<>(new Tuple2<>(data._1, data._2._1), data._2._2))
                .distinct();

        JavaPairRDD<Tuple2<Integer, Integer>, Long> userCourseCountRdd = chapterUserCourse.mapToPair(data -> new Tuple2<>(new Tuple2<>(data._1._2, data._2), 1L))
                .reduceByKey((val1, val2) -> val1 + val2);

        JavaPairRDD<Integer, Long> courseCountRdd = userCourseCountRdd.mapToPair(data -> new Tuple2<>(data._1._2, data._2));

        JavaPairRDD<Integer, Long> courseChapterCountRDD = chapterCourseDataPairRDD.mapToPair(data -> new Tuple2<>(data._2, 1L))
                .reduceByKey((val1, val2) -> val1 + val2);

        JavaPairRDD<Integer, Integer> integerLongJavaPairRDD = courseCountRdd.join(courseChapterCountRDD)
                .mapToPair(data -> new Tuple2<>(data._1, (100 * data._2._1) / data._2._2))
                .mapToPair(data -> new Tuple2<>(data._1, getScore(data._2)))
                .reduceByKey((val1, val2) -> val1 + val2);

        integerLongJavaPairRDD.collect().forEach(System.out::println);

        new Scanner(System.in).nextLine();

        sc.close();
    }

    private static int getScore(float courage){
        if(courage > 90.0){
            return 10;
        }else if(courage > 50.0){
            return 4;
        }else if(courage > 25.0){
            return 2;
        }
        return 0;
    }

}
