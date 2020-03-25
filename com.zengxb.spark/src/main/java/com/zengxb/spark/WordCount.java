package com.zengxb.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {
		// 部署环境
		SparkConf config = new SparkConf();
		config.setMaster("local[*]").setAppName("WordCount");
		// 上下文
		JavaSparkContext sc = new JavaSparkContext(config);
		// 一行一行的读取文件
		JavaRDD<String> lines = sc.textFile("in", 1);
		// 将一行一行的数据分解为一个个单词
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		// 将单词进行结构转换
		JavaPairRDD<String, Integer> wordsToOne = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		// 对转换后的数据进行分组聚合
		JavaPairRDD<String, Integer> wordToSum = wordsToOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		// 在控制台打印
		for (Tuple2<String, Integer> str : wordToSum.collect()) {
			System.out.println(str.toString());
		}
		sc.stop();
		sc.close();
	}
}
