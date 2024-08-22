package com.example.sparkAssingment;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Filter {
    public static void main(String args[]){
        System.setProperty("hadoop.home.dir", "c:/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);


        SparkSession spark = SparkSession.builder().appName("testingsql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();


        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("C:\\Users\\admin\\Downloads\\sparkAssingment\\sparkAssingment\\src\\main\\resources\\heart.csv");

        String expression ="age > 50 AND trestbps > 140 AND CP = 0";

        dataset = dataset.withColumn("finalvalue", functions.expr(expression));
        dataset.show();

        dataset.write()
                .option("header", true)
                .mode("overwrite")
                .csv("C:\\Users\\admin\\Downloads\\sparkAssingment\\sparkAssingment\\output\\filter");

        spark.stop();

    }
}
