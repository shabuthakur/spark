package com.example.sparkAssingment;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Quantize {
    public static void main(String[] args) {


        System.setProperty("hadoop.home.dir", "c:/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("FeatureDerivation")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
                .getOrCreate();


        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("C:\\Users\\admin\\Downloads\\sparkAssingment\\sparkAssingment\\output\\filter");


        dataset = dataset.withColumn("cholesterol_level",
                functions.when(functions.col("chol").lt(200), "Low")
                        .when(functions.col("chol").between(200, 239), "Medium")
                        .otherwise("High"));


        dataset.show();
// ohe ->filter ->quantize ->reduce

        dataset.write().mode("overwrite")
                .csv("C:\\Users\\admin\\Downloads\\sparkAssingment\\sparkAssingment\\output\\Quantize");


        spark.stop();





    }
    }
