package cs523.bdt.team1;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession; 

class App {

	public static void main(String[] args) throws Exception  {
		SparkConf conf = new SparkConf().setAppName("StreamingJob").setMaster("local[1]");

		conf.set("spark.sql.warehouse.dir", "hdfs://127.0.0.1:8020/user/hive/warehouse/");
		conf.set("hive.metastore.uris", "thrift://localhost:9083");
		conf.set("hive.exec.scratchdir", "hdfs://127.0.0.1:8020/user/cloudera/temp/");
		SparkSession ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
		ss.sql("show databases").show();;
		ss.sql("use default").show(false);;
		ss.sql("show tables").show();;

		Scanner kbd = new Scanner (System.in);

		String decision;
		boolean yn = true;
		while (yn) {
			System.out.println("Do you want to run/rerun? : y or n");
			decision = kbd.nextLine();
			switch(decision)
			{
			case "n":
				yn = false;
				break;
			case "y":
				
				FileCollector.merge();
				
				//1.Analysis 1
				ss.sql("DROP TABLE IF EXISTS tb_size_avg").show();
				ss.sql("CREATE TABLE IF NOT EXISTS tb_size_avg(location string, avg string)");
				ss.sql("INSERT INTO tb_size_avg SELECT location, AVG(LENGTH(text)) avg FROM twitterData GROUP BY location");
				Dataset<Row> query = ss.sql("SELECT * FROM tb_size_avg LIMIT 10");
				query.show();

				//1.Analysis 1 - details tb_size_avg_dtls
				ss.sql("DROP TABLE IF EXISTS tb_size_avg_dtls").show();
				ss.sql("CREATE TABLE tb_size_avg_dtls AS SELECT * FROM twitterData");
				query = ss.sql("SELECT * FROM tb_size_avg_dtls LIMIT 10");
				query.show();
				
				//2.Analysis 2
				ss.sql("DROP TABLE IF EXISTS tb_tweets_by_count").show();
				ss.sql("CREATE TABLE IF NOT EXISTS tb_tweets_by_count(location string, avg string)");
				ss.sql("INSERT INTO tb_tweets_by_count SELECT location, COUNT(1) cnt FROM twitterData GROUP BY location");
				query = ss.sql("SELECT * FROM tb_tweets_by_count LIMIT 10");
				query.show();

				//2.Analysis 2 - details
				ss.sql("DROP TABLE IF EXISTS tb_tweets_by_count_dtls").show();
				ss.sql("CREATE TABLE tb_tweets_by_count_dtls(id string, location string)");
				ss.sql("INSERT INTO tb_tweets_by_count_dtls SELECT id, location FROM twitterData");
				query = ss.sql("SELECT * FROM tb_tweets_by_count_dtls LIMIT 10");
				query.show();
				
				
				//3.Analysis 3
				ss.sql("DROP TABLE IF EXISTS tb_popular_tweets").show();
				ss.sql("CREATE TABLE IF NOT EXISTS tb_popular_tweets(tweetid string, tweettext string, retweet_cnt bigint, fav_cnt bigint, screenname string)");
				ss.sql("INSERT INTO tb_popular_tweets SELECT id, text, CAST(retweetcount AS bigint) retweetcount, CAST(favoritecount AS bigint) favoritecount, screenname "
						+ "FROM twitterData WHERE CAST(followerscount AS bigint) > 400 "
						+ "ORDER BY CAST(followerscount AS bigint)");
				query = ss.sql("SELECT * FROM tb_popular_tweets");
				query.show();
			}

		}
	}

}
