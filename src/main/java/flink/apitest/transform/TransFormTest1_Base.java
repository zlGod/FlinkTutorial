package flink.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFormTest1_Base {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> readTextFile = env.readTextFile("/Users/wd/IdeaProjects/FlinkTutorial/src/main/resources/sencor.txt");

        //1.map 把string转换成长度输出
        DataStream<Integer> map = readTextFile.map(new MapFunction<String, Integer>() {
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        //2.flatmap 按逗号切分字段
        DataStream<String> flatMap = readTextFile.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(",");
                for (String s1 : split) {
                    collector.collect(s1);
                }

            }
        });

        //3.filter 筛选sensor_1开头的对应数据
        DataStream<String> filter = readTextFile.filter(new FilterFunction<String>() {
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor_1");
            }
        });

        map.print("map");
        flatMap.print("flatmap");
        filter.print("filter");
        env.execute();

    }
}
