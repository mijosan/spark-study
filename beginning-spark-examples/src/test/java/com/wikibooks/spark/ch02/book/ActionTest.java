package com.wikibooks.spark.ch02.book;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ActionTest {

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

    @Test
    public void test() {
        // given

        // when

        // then
    }

}
