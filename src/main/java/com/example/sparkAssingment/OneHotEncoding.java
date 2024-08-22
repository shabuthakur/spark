package com.example.sparkAssingment;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OneHotEncoding {

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


        StringIndexer indexer = new StringIndexer()
                .setInputCol("slope")
                .setOutputCol("index");

        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCol("index")
                .setOutputCol("vector");

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{indexer, encoder});
        PipelineModel model = pipeline.fit(dataset);
        Dataset<Row> encodedDf = model.transform(dataset);

        encodedDf.show();

        dataset.write()
                .option("header", true)
                .mode("overwrite")
                .csv("C:\\Users\\admin\\Downloads\\sparkAssingment\\sparkAssingment\\output\\one_hot_encoding");

        spark.stop();

    }

}
