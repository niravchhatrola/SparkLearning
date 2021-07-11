package com.chhatrola.sparklearning.sql.fullsql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

/**
 * Created by niv214 on 3/7/21.
 */
public class AggregationGroupBySqlDemo {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder().appName("AggregationGroupBySqlDemo")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "/home/niv214/temp")
                .getOrCreate();

        Dataset<Row> csvDataset = sparkSession.read().option("header", "true")
                .csv("src/main/resources/biglog.txt");


        csvDataset = csvDataset.select(col("level"), date_format(col("datetime"), "MMMM").alias("month"),
                                        date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType))
                        .groupBy(col("level"), col("month"), col("monthnum")).count()
                        .orderBy(col("monthnum"), col("level"));

        csvDataset.show(50);
    }
}
