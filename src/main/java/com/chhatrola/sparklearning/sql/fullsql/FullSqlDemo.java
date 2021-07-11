package com.chhatrola.sparklearning.sql.fullsql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * Created by niv214 on 3/7/21.
 */
public class FullSqlDemo {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder().appName("DetasetDemo")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "/home/niv214/temp")
                .getOrCreate();

        Dataset<Row> csvDataset = sparkSession.read().option("header", "true")
                .csv("src/main/resources/biglog.txt");


        csvDataset.createOrReplaceTempView("my_log_table");

        Dataset<Row> dataset = sparkSession.sql("select distinct(level) from my_log_table order by level desc");
        dataset.show();
    }
}
