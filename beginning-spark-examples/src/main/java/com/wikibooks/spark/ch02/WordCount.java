package com.wikibooks.spark.ch02;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

    public static void main(String[] args) {
        // Step1: SparkContext 생성
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        JavaSparkContext sc = getSparkContext("WordCount", "local[*]");

        try {
            // Step2: 입력 소스로부터 RDD 생성
            JavaRDD<String> inputRDD = getInputRDD(sc, "HELP.MD");
            
            // Step3: 필요한 처리를 수행
            JavaPairRDD<String, Integer> resultRDD = process(inputRDD);

            // Step4: 수행 결과 처리
            handleResult(resultRDD, "resultFile");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Step5: Spark와의 연결 종료
            sc.stop();
        }
    }

    public static JavaSparkContext getSparkContext(String appName, String master) {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);

        return new JavaSparkContext(conf);
    }

    public static JavaRDD<String> getInputRDD(JavaSparkContext sc, String input) {
        return sc.textFile(input); // RDD 생성
    }

    public static JavaPairRDD<String, Integer> process(JavaRDD<String> inputRDD) {
        JavaRDD<String> words = inputRDD.flatMap(s ->
            Arrays.asList(s.split(" "))
            .iterator());
        
        JavaPairRDD<String, Integer> wcPair = words.mapToPair(w -> 
            new Tuple2(w, 1));
        
        JavaPairRDD<String, Integer> result = wcPair.reduceByKey((c1, c2) ->
            c1 + c2);
        
        return result;
    }

    public static void handleResult(JavaPairRDD<String, Integer> resultRDD, String output) {
        resultRDD.saveAsTextFile(output);
    }

}
