package flink.apitest.source;

import flink.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> dataStream = env.addSource( new MySensorSource());
        dataStream.print();
        env.execute();
    }
    //实现自定义的SourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading>{

        //定义一个标识位，用来控制数据的产生
        private boolean running = true;

        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            Random random = new Random();
            HashMap<String, Double> sensorTempMap = new HashMap<String, Double>();
            for(int i = 1;i < 11;i++){
                sensorTempMap.put("sensor"+i,random.nextGaussian() * 20 + 60);
            }


            while (running){

                for(String sensorId:sensorTempMap.keySet()){
                    double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId,newTemp);
                    sourceContext.collect(new SensorReading(sensorId,System.currentTimeMillis(),newTemp));

                    Thread.sleep(1000l);
                }


            }

        }

        public void cancel() {
            running = false;

        }
    }
}

