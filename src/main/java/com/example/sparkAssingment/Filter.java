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
                .csv("C:\\Users\\admin\\Downloads\\sparkAssingment\\sparkAssingment\\output\\heart.csv");

//        String expression ="age > 50 AND trestbps > 140 AND CP = 0"; // new data set bnana hai

        dataset =dataset.filter(dataset.col("age").gt(50)
                .and(dataset.col("trestbps").gt(140))
                .and(dataset.col("CP").equalTo(0)));
        dataset.show();

        dataset.write()
                .option("header", true)
                .mode("overwrite")
                .csv("C:\\Users\\admin\\Downloads\\sparkAssingment\\sparkAssingment\\output\\filter");

        spark.stop();

    }
}
