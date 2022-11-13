package com.btk.spark.api;

import com.btk.spark.Util;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class Spark {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C://hadoop-common-2.2.0-bin-master");
        /*
        // Create input data
        List<Integer> inputData = new ArrayList<>();
        inputData.add(1);
        inputData.add(2);
        inputData.add(3);
        inputData.add(4);

         */

        // get rid of a bunch of unnecessary log record
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Configuration of Spark
        // give an app name
        // to benefit all core in local, put [*], otherwise single thread that means you are not using full performance of your local.
        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");


        // Connection to our Spark cluster.
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load Java list into RDD.
        // parallelize : loading a collection and turning to RDD
        //JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        //reduce -> in this case factorize
        //Double reduce = myRdd.reduce((value1, value2) -> value1 * value2);
        //System.out.println(reduce);

        //map -> in this case square root
        //JavaRDD<Double> square = myRdd.map(Math::sqrt);
        //square.foreach((VoidFunction<Double>) (value) -> System.out.println(value));

        // Pair RDD
        List<String> logRecords = getLogging();
        JavaRDD<String> myRdd = sc.parallelize(logRecords);

        /*
        JavaPairRDD<String, Long> logRdd = myRdd.mapToPair( rawValue -> {
            String[] columns = rawValue.split(": ");

            String level = columns[0];

            return new Tuple2<>(level, 1L);
        });

        // count each record type
        JavaPairRDD<String, Long> sumsRdd = logRdd.reduceByKey(Long::sum);
        sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

         */

        // count each record type -> equivalent to the lines of codes as above except this is shorter and fluent
        sc.parallelize(logRecords)
                .mapToPair( rawValue -> new Tuple2<>(rawValue.split(": ")[0], 1L))
                .reduceByKey(Long::sum)
                .foreach((VoidFunction<Tuple2<String, Long>>) tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        // count each record type : using groupByKey -> problamatic
        sc.parallelize(logRecords)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(": ")[0], rawValue.split(": ")[1]))
                .groupByKey()
                .foreach( stringIterableTuple2 -> System.out.println(stringIterableTuple2._1 + " has " + stringIterableTuple2._2.spliterator().getExactSizeIfKnown()) );

        // flatmap - filter
        sc.parallelize(logRecords).flatMap(value -> Arrays.asList(value.split(": ")).iterator())
                .filter(th -> th.contains("Tuesday"))
                .collect()
                .forEach(System.out::println);

        // read text file
        JavaPairRDD<Long, String> subtitleWordCount = sc.textFile("src//main//resources//subtitles//input.txt")
                .map(value -> value.replaceAll("[^a-zA-Z\\s]", "").toLowerCase(Locale.ROOT))
                .filter(sentence -> sentence.trim().length() > 0)
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(val -> val.trim().length() > 0)
                .filter(Util::isNotBoring)
                .mapToPair(notBoring -> new Tuple2<>(notBoring, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false);

        subtitleWordCount.take(50).forEach(System.out::println);





        System.out.println(myRdd.count());
        sc.close();
    }

    protected static List<String> getLogging(){

        String[] records = {"WARN: Tuesday 5 September 0405",
                        "WARN: Tuesday 4 September 0406",
                        "ERROR: Tuesday 4 September 0408",
                        "FATAL: Wednesday 5 September 1632",
                        "ERROR: Friday 7 September 1854",
                        "WARN: Saturday 8 sEPTEMBER 1942"
                        };

        return Arrays.asList(records);
    }
}
