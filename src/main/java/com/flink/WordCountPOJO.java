package com.flink;

import com.flink.entity.WordInfo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 使用POJO，存储text中的信息数据
 */
public class WordCountPOJO {
    public static void main(String[] args) {
        //获取当前运行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> textSet = env.readTextFile("/Users/gumeifeng/com.flink/src/main/resources/text.txt");

        DataSet<WordInfo> counts = textSet.flatMap(new FlatMapFunction<String, WordInfo>() {
                    @Override
                    public void flatMap(String s, Collector<WordInfo> collector) throws Exception {
                        //string s为读取到的一行
                        String[] tokens = s.toLowerCase().split(" ");
                        for (String token : tokens) {
                            if (token.length() > 0) {
                                collector.collect(new WordInfo(token, 1));
                            }
                        }
                    }
                })
                        //使用实体的word字段，排序
                        .groupBy(WordInfo::getWord)
                        //reduce收集、汇总
                        .reduce(new ReduceFunction<WordInfo>() {
                            @Override
                            public WordInfo reduce(WordInfo wordInfo, WordInfo t1) throws Exception {
                                return new WordInfo(wordInfo.getWord(), wordInfo.getCount() + t1.getCount());
                            }
                        });


        try {
            List<WordInfo> list = counts.collect();
            for (int i = 0; i < list.size(); i++) {
                System.out.println("文本中第" + (i + 1) + "个Word统计为：" + list.get(i).getWord() + ":" + list.get(i).getCount());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
