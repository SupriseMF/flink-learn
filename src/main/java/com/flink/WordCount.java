package com.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * flink  计数
 * @author gumeifeng
 * @date  20191206
 */
public class WordCount {
    public static void main(String[] args) {
        //获取当前运行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> textSet = env.readTextFile("/Users/gumeifeng/com.flink/src/main/resources/text.txt");

        DataSet<Tuple2<String, Integer>> counts =
                textSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        //string s为读取到的一行
                        String[] tokens = s.toLowerCase().split(" ");
                        for (String token : tokens) {
                            if (token.length() > 0) {
                                collector.collect(new Tuple2<>(token, 1));
                            }
                        }
                    }
                })
                        .groupBy(0)
                        .aggregate(Aggregations.SUM, 1);


        try {
            List<Tuple2<String, Integer>> list = counts.collect();
            for (int i = 0; i < list.size(); i++) {
                System.out.println("文本中第" + (i + 1) + "个Word统计为：" + list.get(i).f0 + ":" + list.get(i).f1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
