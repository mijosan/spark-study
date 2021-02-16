package com.wikibooks.spark.ch02.study;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import scala.Tuple2;

public class RDDTest {

    private static SparkConf conf;
    private static JavaSparkContext sc;

    @BeforeEach
    void setUp() {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        conf = new SparkConf().setAppName("WordCountTest").setMaster("local[*]");
        sc = new JavaSparkContext(conf);
    }

    @AfterEach
    void cleanUp() {
        if (sc != null) {
            sc.stop();
        }
    }

    @DisplayName("각 단어의 횟수를 내림차순으로 정렬")
    @Test
    public void wordCountExample() {
        JavaRDD<String> lines = sc.textFile("src/test/java/com/example/ch02/data/word_count.text");
        JavaRDD<String> word = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // word.collect().stream()
        //     .forEach(System.out :: println);
        
        // for (int i = 0; i < word.collect().size(); i++) {
        //     System.out.println(word.collect().get(i));
        // }

        JavaPairRDD<String, Integer> wordPairRDD = word.mapToPair(w -> new Tuple2<>(w, 1));

        // wordPairRDD.foreach(w -> System.out.println(w._1() + " " + w._2()));

        JavaPairRDD<String, Integer> reducePairRDD = wordPairRDD.reduceByKey((x, y) -> x + y);

        // reducePairRDD.foreach(w -> System.out.println(w._1() + " " + w._2()));

        JavaPairRDD<Integer, String> wordPairRDD2 = reducePairRDD.mapToPair(w -> new Tuple2<>(w._2(), w._1())).sortByKey(false);

        JavaPairRDD<String, Integer> orderedPairRDD = wordPairRDD2.mapToPair(w -> new Tuple2<>(w._2(), w._1()));

        orderedPairRDD.collect().stream()
            .forEach(System.out :: println);
        
        assertThat(orderedPairRDD.collectAsMap().get("the")).isEqualTo(71);
    }


}
