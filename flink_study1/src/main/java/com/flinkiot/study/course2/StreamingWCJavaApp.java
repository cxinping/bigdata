package com.flinkiot.study.course2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用Java API来开发Flink的实时处理应用程序.
 *
 * wc统计的数据我们源自于socket
 */
public class StreamingWCJavaApp {
    public static void main(String[] args) throws Exception {

        // step1 ：获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // step2：读取数据
        DataStreamSource<String> text = env.socketTextStream("192.168.11.12", 9999);

        // step3: transform

        //text.flatMap(new MyFlatMapFunction())   //.keyBy("word")
        text.flatMap(new RichFlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String value, Collector<WC> collector) throws Exception {
                String[] tokens = value.toLowerCase().split(",");
                for(String token : tokens) {
                    if(token.length() > 0) {
                        collector.collect(new WC(token.trim(), 1));
                    }
                }
            }
        })
          .keyBy(new KeySelector<WC, String>() {

              @Override
              public String getKey(WC wc) throws Exception {
                  return wc.word;
              }
          })
                //.timeWindow(Time.seconds(5))
                //test
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))

                .sum("count").print()
                .setParallelism(1);


        env.execute("StreamingWCJavaApp");
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, WC> {
        @Override
        public void flatMap(String value, Collector<WC> collector) throws Exception {
            String[] tokens = value.toLowerCase().split(",");
            for(String token : tokens) {
                if(token.length() > 0) {
                    collector.collect(new WC(token.trim(), 1));
                }
            }
        }
    }

    public static class WC {
        private String word;
        private int count;

        public WC(){}

        public WC(String word,int count ){
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
}
