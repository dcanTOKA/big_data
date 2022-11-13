package com.btk.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class SparkSQL{

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C://hadoop-common-2.2.0-bin-master");
        Logger.getLogger("org.apache").setLevel(Level.WARN);


        // create java spark context
        /*

        SparkConf conf = new SparkConf()
                .setAppName("startingSqlSpark")
                .setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

         */

        SparkSession spark = SparkSession.builder()
                .appName("startingSqlSpark")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

        //print first 20 rows
        //dataSet.show();

        // get first row and print as a json
        Row firstRow = dataSet.first();
        System.out.println(firstRow.prettyJson());

        // filter using expression and column

        //Dataset<Row> filter = dataSet.filter("grade = 'B' and subject = 'French'");
        Dataset<Row> filter = dataSet.filter(
                col("grade").equalTo("B")
                .and(
                        col("subject").equalTo("French")));

        // filter using lambda

        // Dataset<Row> filter = dataSet.filter((FilterFunction<Row>) row -> row.getAs("grade").equals("B"))
        //       .filter((FilterFunction<Row>) row -> row.getAs("subject").equals("French"));

        // filter using sql syntax
        dataSet.createOrReplaceTempView("my_table");
        Dataset<Row> sql = spark.sql("select * from my_table where grade = 'B' and subject='French'");

        sql.show();




        spark.close();
    }
}
