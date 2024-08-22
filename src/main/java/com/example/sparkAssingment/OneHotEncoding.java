package com.example.sparkAssingment;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
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

        StringIndexer indexerCP = new StringIndexer()
                .setInputCol("cp")
                .setOutputCol("CPIndex");

        OneHotEncoder encoderCP = new OneHotEncoder()
                .setInputCol("CPIndex")
                .setOutputCol("CPVec");


        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{
                        indexerCP, encoderCP,
                });


        PipelineModel model = pipeline.fit(dataset);
        Dataset<Row> encodedDf = model.transform(dataset);

        encodedDf.show();

        encodedDf.write()
                .option("header", true)
                .mode("overwrite")
                .parquet("C:\\Users\\admin\\Downloads\\sparkAssingment\\sparkAssingment\\output\\one-hot-encoding");

        spark.stop();

    }

}
