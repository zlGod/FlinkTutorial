package flink.apitest.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String path = "/Users/zl/IdeaProjects/FlinkTutorial/src/main/resources/hello.txt";
        DataSet<String> dataSource = environment.readTextFile(path);

        DataSet<Tuple2<String, Integer>> sum = dataSource.flatMap(new MyFlatMapper()).groupBy(0).sum(1);
        sum.print();
    }

}

class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{

    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] split = s.split(" ");
        for(String s1:split){
            collector.collect(new Tuple2<String,Integer>(s1,1));
        }
    }
}
