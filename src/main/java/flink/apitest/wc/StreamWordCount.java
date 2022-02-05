package flink.apitest.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        environment.setParallelism(1);
        environment.disableOperatorChaining();//全局不共享插槽，算子拒绝合并Operator Chains

        ParameterTool fromArgs = ParameterTool.fromArgs(args);
        String host = fromArgs.get("host");
        int port = fromArgs.getInt("port");

        DataStream<String> streamSource = environment.socketTextStream(host,port);

//        String path = "/Users/wd/IdeaProjects/FlinkTutorial/src/main/resources/hello.txt";
//        DataStream<String> streamSource = environment.readTextFile(path);
        DataStream<Tuple2<String, Integer>> outputStreamOperator = streamSource.flatMap(new MyFlatMapper()).keyBy(0).sum(1)
              ;//.startNewChain();在该算子后重新开始新的任务链合并
        //  .disableChaining();可在算子后进行禁止共享
        outputStreamOperator.print();
        environment.execute();
    }
}
