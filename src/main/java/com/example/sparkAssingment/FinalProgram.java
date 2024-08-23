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
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class FinalProgram {

    public static void main(String args[]) {

        System.setProperty("hadoop.home.dir", "c:/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder().appName("testingsql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .csv("C:\\Users\\admin\\Downloads\\sparkAssingment\\sparkAssingment\\src\\main\\resources\\heart.csv");

        // One-Hot Encoding
        StringIndexer indexerCP = new StringIndexer()
                .setInputCol("cp")
                .setOutputCol("CPIndex");

        OneHotEncoder encoderCP = new OneHotEncoder()
                .setInputCol("CPIndex")
                .setOutputCol("CPVec");

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{indexerCP, encoderCP});

        // Feature Derivation
        dataset = dataset.withColumn("powerOfTrestbps", functions.pow(dataset.col("trestbps"), 2));

        PipelineModel model = pipeline.fit(dataset);
        dataset = model.transform(dataset);

        // Filter
        dataset = dataset.filter(dataset.col("age").gt(50)
                .and(dataset.col("trestbps").gt(140))
                .and(dataset.col("CP").equalTo(0)));

        // Quantize
        dataset = dataset.withColumn("cholesterol_level",
                functions.when(functions.col("chol").lt(200), "Low")
                        .when(functions.col("chol").between(200, 239), "Medium")
                        .otherwise("High"));

        // Reduce
        Long count = dataset.filter(dataset.col("cholesterol_level").equalTo("Low")).count();
        System.out.println("Low risk: " + count);

        dataset.show();

        spark.udf().register("vectorToString", (UDF1<Vector, String>) vector -> {
            if (vector != null) {
                return vector.toString();
            } else {
                return "";
            }
        }, DataTypes.StringType);

        Dataset<Row> convertedDf = dataset.withColumn("CPVector",
                functions.callUDF("vectorToString", dataset.col("CPVec")));

        convertedDf = convertedDf.drop("CPVec");

        convertedDf.write()
                .option("header", true)
                .mode("overwrite")
                .csv("C:\\Users\\admin\\Downloads\\sparkAssingment\\sparkAssingment\\output\\filter");

        spark.stop();
    }
}
