/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import org.apache.spark.streaming.kafka.KafkaUtils;

import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.util.*;
import java.sql.Timestamp;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;

public class Consumer {

    public static void main(String[] args) throws InterruptedException {
        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("KafkaConsumer");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        int numThreads = 4;
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put("tweet", numThreads);

        JavaPairReceiverInputDStream<String, String> messages
                = KafkaUtils.createStream(jssc, "localhost", "group1", topicMap);

        JavaDStream<String> lines = messages.map(Tuple2::_2);
        JavaDStream<tweetConsumer> tweets = lines.map(x -> createObject(x));
        JavaDStream<tweetConsumer> tweetsWithSentiments = tweets.map(x -> performSentimentAnalysis(x));
        JavaDStream<String> twits = tweetsWithSentiments.map(x -> x.toString());
        JavaEsSparkStreaming.saveToEs(tweetsWithSentiments, "bigdata/docs",ImmutableMap.of(new Timestamp(System.currentTimeMillis()).toString(), "timestamp"));
        twits.print();
        jssc.start();
        jssc.awaitTermination();
    }

    public static tweetConsumer createObject(String s){
        Gson gson = new Gson();
        tweetConsumer t = new tweetConsumer();
        t = gson.fromJson(s, tweetConsumer.class);
        t.location = t.latitude + "," + t.longitude;
        return t;
    }
    public static tweetConsumer performSentimentAnalysis(tweetConsumer t) {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        int mainSentiment = 0;
        if (t.getMsg() != null && t.getMsg().length() > 0) {
            int longest = 0;
            Annotation annotation = pipeline.process(t.getMsg());
            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        switch (mainSentiment) {
            case 2:
                t.setSentiment("NEUTRAL");
                break;
            case 1:
            case 0:
                t.setSentiment("NEGATIVE");
                break;
            case 3:
            case 4:
                t.setSentiment("POSITIVE");
                break;
            default:
                break;
        }
        return t;
    }
}