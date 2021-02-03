package cs523.bdt.team1;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
//import KafkaProducer packages
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import cs523.bdt.team1.callback.BasicCallback;
import cs523.bdt.team1.model.Tweet;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;


public class TweeterProducer{
	
    public final String CONSUMER_KEY = "SmMk7ITT2eMvqCsPb27rkaEbN";
    public final String CONSUMER_SECRET = "CvZIXCGvQHPEIG9Dx2T0fB3qF5sJQHobrkFZ00Fm7hEdRZY5HN";
    public final String ACCESS_TOKEN = "203372796-TYVVQjpPL6rOpv3G8Dl8ajrMoHKltXAYHLmVH3u7";
    public final String TOKEN_SECRET = "0QowaORv3WKIbnIUXjJs848zSRyFHTCgNxHKFDHgcJ2VK";
    public final String HASHTAG = "#DOGE";
    
    public static String SERVERS = "0.0.0.0:9092";
    public static String TOPIC = "t1";
    public static long SLEEP_TIMER = 1000;
    
    private Client client;
    private BlockingQueue<String> queue;
    private Gson gson;
    private Callback callback;

	InputStream inputStream;

    
    public TweeterProducer() {
        // Configure auth
        Authentication authentication = new OAuth1(
        		CONSUMER_KEY,
                CONSUMER_SECRET,
                ACCESS_TOKEN,
                TOKEN_SECRET);

        // track the terms of your choice. here im only tracking #bigdata.
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Collections.singletonList(HASHTAG));

        queue = new LinkedBlockingQueue<>(10000);
        client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue))
                .build();
        gson = new Gson();
        callback = new BasicCallback();
    }

    private Producer<Long, String> getProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<Long, String>(properties);
    }

    public void run() {
        client.connect();
        int count = 0;
        try (Producer<Long, String> producer = getProducer()) {
            while (true) {
                Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
                System.out.println(tweet);
//                System.out.printf("Fetched tweet id %d\n", tweet.getId());
//                System.out.println("All info: " + tweet.toString());
//                System.out.println("Count " + count++);
//                System.out.println("Count " +tweet.getFavoriteCount());

                long key = tweet.getId();
                String msg = tweet.toString();
                ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, key, msg);
                
                producer.send(record, callback);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
    }
    
}

