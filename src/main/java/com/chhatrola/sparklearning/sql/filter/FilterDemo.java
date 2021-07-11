package com.chhatrola.sparklearning.sql.filter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/**
 * Created by niv214 on 3/7/21.
 */
public class FilterDemo {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder().appName("DetasetDemo")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "/home/niv214/temp")
                .getOrCreate();

        Dataset<Row> csvDataset = sparkSession.read().option("header", "true")
                .csv("src/main/resources/biglog.txt");

        Dataset<Row> debugDataset = csvDataset.filter("level = 'DEBUG'");

        debugDataset.show();

        Dataset<Row> infoDataset = csvDataset.filter(row -> row.getAs("level").equals("INFO"));
        infoDataset.show();

        Dataset<Row> errorDataset = csvDataset.filter(col("level").equalTo("ERROR"));
        errorDataset.show();

    }
}
