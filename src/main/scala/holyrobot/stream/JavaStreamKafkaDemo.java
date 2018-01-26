package holyrobot.stream;


import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class JavaStreamKafkaDemo {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        //提交集群模式注释打开
     /*   if (args.length == 0) {
            System.err
                    .println("Usage: SparkStreamingFromFlumeToHBaseWindowingExample {master} {host} {port} {table} {columnFamily} {windowInSeconds} {slideInSeconds");
            System.exit(1);
        }
*/

        String zkQuorum = "min1:2181";
        String group = "group";
        String topicss = "test0806";
        String numThread = "2";

        //init conf jssc
        SparkConf sparkConf = new SparkConf().setAppName("JavaStreamkafka");
        //集群模式注释掉
        sparkConf.setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        int numThreads = Integer.parseInt(numThread);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = topicss.split(",");
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        JavaDStream<String> lines =
                messages.map(new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> tuple2) {
                        return tuple2._2();
                    }
                });

        JavaDStream<String> words =
                lines.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String x) {
                        return Lists.newArrayList(SPACE.split(x));
                    }
                });

        words.dstream();
        words.print();
        jssc.start();
        jssc.awaitTermination();
    }

}
