package com.wikibooks.spark.ch05;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSessionSample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("Sample")
            .master("local[*]")
            .getOrCreate();
        
        String source = "file://<spark_home_dir>/README.md";

        // DataFrame
        Dataset<Row> df = spark.read().text(source);
        
        // Dataset<Row> wordDF = df.select(explode(split(col("value"), " ")).as("word"));

        // Dataset
        Dataset<String> ds = df.as(Encoders.STRING());

        // Dataset<String> wordDF = ds.flatMap(str -> Arrays.asList(str.split(" ")).iterator(), Encoders.STRING());
        Dataset<String> wordDF = ds.flatMap(new FlatMapFunction<String , String>() { 
            @Override 
            public Iterator<String> call(String v) throws Exception { 
                return Arrays.asList(v.split(" ")).iterator(); 
            }
        }, Encoders.STRING());
        
        
    }

}
