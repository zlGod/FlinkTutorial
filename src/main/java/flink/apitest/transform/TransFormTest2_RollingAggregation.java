package flink.apitest.transform;

import flink.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFormTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStream = env.readTextFile("/Users/wd/IdeaProjects/FlinkTutorial/src/main/resources/sencor.txt");

        DataStream<SensorReading> map = dataStream.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[] splits = s.split(",");
                return new SensorReading(splits[0], new Long(splits[1]), new Double(splits[2]));
            }
        });
        KeyedStream<SensorReading, Tuple> id = map.keyBy("id");
        KeyedStream<SensorReading, Object> keyedStream = map.keyBy(SensorReading::getId);


        env.execute();
    }
}
