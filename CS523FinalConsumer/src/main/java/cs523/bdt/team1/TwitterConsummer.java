package cs523.bdt.team1;


import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.api.java.JavaRDD;


import scala.Tuple2;

public class TwitterConsummer {

	public static void main(String[] args)  {

		String zkQuorum = "localhost:2181";
		
		SparkConf sparkConf = new SparkConf().setAppName("Twitter").setMaster("local[2]");
		JavaSparkContext ctx = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
		JavaStreamingContext jssc = new JavaStreamingContext(ctx, Durations.milliseconds(10000));
		jssc.sparkContext().setLogLevel("ERROR");
		
		// jssc.checkpoint(checkpointDir);
		// <list columns,values>
		String topics = "t1";
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("zookeeper.connect", "localhost" + ":2181");
		kafkaParams.put("group.id", "group1");
		kafkaParams.put("auto.offset.reset", "smallest");
		kafkaParams.put("metadata.broker.list", "locahost" + ":9092");
		kafkaParams.put("bootstrap.servers", "locahost" + ":9092");
		Map<String, Integer> topics1 = new HashMap<String, Integer>();
		topics1.put("t1", 1);
		JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(jssc, zkQuorum, "twitter", topics1);

		JavaDStream<Tuple2<String, String>> text = kafkaStream.toJavaDStream();

		text.foreachRDD((rdd, time) -> {
			JavaRDD<String> plainRDD = rdd.coalesce(1).map(tuple -> (String) tuple._2()).map(s -> s.replaceAll("[^\\x00-\\x7E]|\\r|\\n|\\r\\n", " "));
			sendDataToHDFS(plainRDD, false);
		});
		
		text.print();
		
		jssc.start();
		jssc.awaitTermination();
	}
	
	
	public static void sendDataToHDFS(JavaRDD<String> plainRDD, boolean toTwoTables){
		//toTwoTables = true if partitioning data into TWO hive tables, false if only ONE hive table
		
		//in case of having two tables: users and tweets
		if(toTwoTables){
			JavaRDD<String> u = plainRDD.map(s -> s.replaceAll("<ENDTWEET>","").split("<BEGINUSER>")[1]);
			JavaRDD<String> t = plainRDD.map(s -> s.replaceAll("<BEGINUSER>","").split("<ENDTWEET>")[0]);
			
			
			u.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/hive_tables/users/"+System.currentTimeMillis());
			t.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/hive_tables/tweets/"+System.currentTimeMillis());
		}
		//in case of having two tables: users and tweets
		else
//			plainRDD.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/hive_tables/all_info/");//+System.currentTimeMillis());
			plainRDD.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/temp_info/" + System.currentTimeMillis());

	}
	
}