package com.chhatrola.sparklearning.sql.dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by niv214 on 3/7/21.
 */
public class DetasetDemo {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder().appName("DetasetDemo")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "/home/niv214/temp")
                .getOrCreate();

        Dataset<Row> csvDataset = sparkSession.read().option("header", "true")
                .csv("src/main/resources/biglog.txt");

        csvDataset.show();
        long count = csvDataset.count();
        System.out.println("Total : "+count);

        Row first = csvDataset.first();
        String lebel = first.get(0).toString();

        System.out.println(lebel);

    }
}
