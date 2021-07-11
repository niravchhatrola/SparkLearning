package com.chhatrola.sparklearning.sql.ops;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by niv214 on 3/7/21.
 */
public class SparkSqlInMemory {
    public static void main(String[] args) {
        List<Row> inMemory = new ArrayList<Row>();
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sparkSession = SparkSession.builder().appName("GettingStartedSparkSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","/home/niv214/temp/")
                .getOrCreate();

        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datatime", DataTypes.StringType, false, Metadata.empty())
        };

        Dataset<Row> dataset = sparkSession.createDataFrame(inMemory, new StructType(fields));
        dataset.show();
    }
}
