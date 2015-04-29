package com.tikalk.bigdata;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PageCount {
    private static final FlatMapFunction<String, String> WORDS_EXTRACTOR =
            new FlatMapFunction<String, String>() {
 				private static final long serialVersionUID = 1L;

				public Iterable<String> call(String s) throws Exception {
                    return Arrays.asList(s.split("[\\r\\n]+"));
                }
            };

    private static final PairFunction<String, String, Integer> WORDS_MAPPER =
            new PairFunction<String, String, Integer>() {
 				private static final long serialVersionUID = 1L;

				public Tuple2<String, Integer> call(String s) throws Exception {
                    List<String> words= Arrays.asList(s.split(" "));
                    if(words==null || words.isEmpty() || words.size()!=4)
                        return  new Tuple2<String, Integer>(" ",0);
                    else if (words.get(0).equals("en"))
                        return new Tuple2<String, Integer>(words.get(1),Integer.parseInt(words.get(3)));
                    else
                        return new Tuple2<String, Integer>(" ",0);
               }
            };

    private static final Function2<Integer, Integer, Integer> WORDS_REDUCER =
            new Function2<Integer, Integer, Integer>() {
    			private static final long serialVersionUID = 1L;

				public Integer call(Integer a, Integer b) throws Exception {
                    return a + b;
                }
            };

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Please provide the input file full path as argument");
            System.exit(0);
        }

        SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> file = context.textFile(args[0]);
        JavaRDD<String> lines = file.flatMap(WORDS_EXTRACTOR);

        JavaPairRDD<String, Integer> pairs = lines.mapToPair(WORDS_MAPPER);
        JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER,1);

        counter.saveAsTextFile(args[1]);
        context.close();
    }
}
