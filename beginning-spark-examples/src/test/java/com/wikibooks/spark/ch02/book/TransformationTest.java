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

import scala.Tuple2;

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
    public void groupByKeyTest() {
        // given
        List<Tuple2<String, Integer>> data = Arrays.asList(new Tuple2("a", 1), new Tuple2("b", 1), new Tuple2("c", 1));
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data);

        // when
        JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd1.groupByKey();

        // then
        System.out.println(rdd2.collect());
        assertThat(rdd2.collect().get(0)._1()).isEqualTo("a");
        assertThat(rdd2.collect().get(1)._1()).isEqualTo("b");
        assertThat(rdd2.collect().get(2)._1()).isEqualTo("c");
    }

    @Test
    public void groupByTest() {
        // given
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        // when
        JavaPairRDD<String, Iterable<Integer>> rdd2 = rdd1.groupBy(v -> (v % 2 == 0) ? "even" : "odd");

        // then ? collect를 여러번 쓰면 여러번의 트랜스포메이션 연산이 일어나나 ?
        System.out.println(rdd2.collect());
        assertThat(rdd2.collect().get(0)._1()).isEqualTo("even");
        assertThat(rdd2.collect().get(1)._1()).isEqualTo("odd");
    }

    // ? a1 b2 c 가 아니고 a b1 c2 인 이유..
    @Test
    public void zipPartitionTest() {
        // given
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"), 3);
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2), 3);

        // when 
        JavaRDD<String> rdd3 = rdd1.zipPartitions(rdd2, (Iterator<String> t1, Iterator<Integer> t2) -> {
            List<String> list = new ArrayList<>();
            t1.forEachRemaining((String s) -> {
                if (t2.hasNext()) {
                    list.add(s + t2.next());
                } else {
                    list.add(s);
                }
            });
            
            return list.iterator();
        });

        // then
        System.out.println(rdd3.collect());
    }
    @Test
    public void zipTest() {
        // given
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3));

        // when
        JavaPairRDD<String, Integer> result = rdd1.zip(rdd2);
        
        // then
        System.out.println(result.collect());
    }

    @Test
    public void flatMapvalueTest() {
        // given
        List<Tuple2<Integer, String>> data = Arrays.asList(new Tuple2(1, "a,b"), new Tuple2(2, "a,c"), new Tuple2(1, "d,e"));

        // when
        JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(data);
        
        JavaPairRDD<Integer, String> rdd2 = rdd1.flatMapValues(v -> Arrays.asList(v.split(",")).iterator());

        // then
        System.out.println(rdd2.collect());
        assertThat(rdd2.collect().size()).isEqualTo(6);
    }

    @Test
    public void mapValuesTest() {
        // given
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c"));

        // when
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(t -> new Tuple2<String, Integer>(t, 1))
            .mapValues(v -> v + 1);
        
        // then
        System.out.println(rdd2.collect());
        assertThat(rdd2.collect().get(0)._2()).isEqualTo(2);
        assertThat(rdd2.collect().get(1)._2()).isEqualTo(2);
        assertThat(rdd2.collect().get(2)._2()).isEqualTo(2);
    }

    @Test
    public void mapPartitionsWithIndexTest() {
        // given
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);

        // when
        JavaRDD<Integer> rdd2 = rdd1.mapPartitionsWithIndex((Integer idx, Iterator<Integer> numbers) -> {
            List<Integer> result = new ArrayList<Integer>();

            if (idx == 0) {
                numbers.forEachRemaining(i -> result.add(i + 1));
            }
            
            return result.iterator();
        }, true);

        // then
        System.out.println(rdd2.collect());
        assertThat(rdd2.collect().get(0)).isEqualTo(2);
        assertThat(rdd2.collect().get(1)).isEqualTo(3);
        assertThat(rdd2.collect().get(2)).isEqualTo(4);
    }

    // 각 파티션 요소에 대한 이터레이터를 전달받아 함수 내부에서 파티션의 개별 요소에 대한
    // 작업을 처리하고 그 결과를 다시 이터레이터 타입으로 되돌려줘야 합니다.
    @Test
    public void mapPartitionTest() {
        // given
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3); 

        // when
        JavaRDD<Integer> rdd2 = rdd.mapPartitions(n -> {
            System.out.println("DB연결 !!!");
            List<Integer> result = new ArrayList<Integer>();
            n.forEachRemaining(i ->
                result.add(i + 1));
            return result.iterator();
        });

        // then
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
