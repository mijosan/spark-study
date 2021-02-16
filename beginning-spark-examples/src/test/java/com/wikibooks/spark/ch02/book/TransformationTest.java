package com.wikibooks.spark.ch02.book;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.wikibooks.spark.ch02.WordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TransformationTest {

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
    public void mapPartitionsWithIndexTest() {
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);

        JavaRDD<Integer> rdd2 = rdd1.mapPartitionsWithIndex((Integer idx, Iterator<Integer> numbers) -> {
            List<Integer> result = new ArrayList<Integer>();

                if (idx == 0) {
                    numbers.forEachRemaining(i -> result.add(i + 1));
                }
                
                return result.iterator();
            }, true);

            assertThat(rdd2.collect().get(0)).isEqualTo(2);
    }

    // 각 파티션 요소에 대한 이터레이터를 전달받아 함수 내부에서 파티션의 개별 요소에 대한
    // 작업을 처리하고 그 결과를 다시 이터레이터 타입으로 되돌려줘야 합니다.
    @Test
    public void mapPartitionTest() {
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3); 
        JavaRDD<Integer> rdd2 = rdd.mapPartitions(n -> {
            System.out.println("DB연결 !!!");
            List<Integer> result = new ArrayList<Integer>();
            n.forEachRemaining(i ->
                result.add(i + 1));
            return result.iterator();
        });

        assertThat(rdd2.collect().get(0)).isEqualTo(2);
    }

    @Test
    public void flatMapTest() {
        // given
        List<String> data = new ArrayList<String>(); 
        data.add("apple,orange"); 
        data.add("grape,apple,mango"); 
        data.add("blueberry,tomato,orange"); 

        JavaRDD<String> rdd = sc.parallelize(data);

        // when
        JavaRDD<String> rdd2 = rdd.flatMap(s ->
            Arrays.asList(s.split(","))
            .iterator());
        System.out.println(rdd2.collect());
    }

    // map
    @Test
    public void mapTest() {
        // given
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // when
        JavaRDD<Integer> rdd2 = rdd.map(s -> 
            s + 1);
        
        // then
        assertThat(rdd2.collect().get(0)).isEqualTo(2);
        System.out.println(org.apache.commons.lang.StringUtils.join(rdd2.collect(), ", "));
    }

    @Test
    public void countTest() {
        // given
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // when
        long result = rdd.count();

        // then
        assertThat(result).isEqualTo(5);
    }

    @Test
    public void testProcess() {
        // given
        List<String> input = new ArrayList<String>();
        input.add("Apache Spark is a fast and general engine for large-scale data processing.");
        input.add("Spark runs on both Windows and UNIX-like system");

        JavaRDD<String> inputRDD = sc.parallelize(input);
        
        // when
        JavaPairRDD<String, Integer> resultRDD = WordCount.process(inputRDD);

        // then
        Map<String, Integer> resultMap = resultRDD.collectAsMap();

        assertThat(2).isEqualTo(resultMap.get("Spark"));
        assertThat(2).isEqualTo(resultMap.get("and"));
        assertThat(1).isEqualTo(resultMap.get("runs"));

        System.out.println(resultMap);
    }

}
