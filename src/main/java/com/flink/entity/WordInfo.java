package com.flink.entity;


public class WordInfo {
    private String word;
    private Integer count;

    public WordInfo(String name, Integer count) {
        this.word = name;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
