/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import com.google.gson.Gson;
import java.util.Properties;
import java.util.Random;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * @author shrut
 */
public class TwitterScrapper {

    public static void main(String[] args) throws Exception {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("CC0fE4peReUpxWJUjWHdLDBk5")
                .setOAuthConsumerSecret("dojOw6xURRxNNdRBIB6bwvKGjg1PF2TGypudzOtzs3oArjBFhr")
                .setOAuthAccessToken("247276397-6ZHGjHtUCnGjb9rygRGfjnKZstT6BG3lKZtcHYr6")
                .setOAuthAccessTokenSecret("PaJMWulEBziCCl2jAKyBp7H2FBTiIagNlefehFkgR5KO2");
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                tweet t = new tweet();
                Gson gson = new Gson();
                double[] locationLatitude = {32.794176, 39.761849, 37.364613, 40.664274, 33.762909};
                double[] locationLongitude = {-96.765503, -104.880625, -121.967934, -73.9385, -84.422675};
                Properties props = new Properties();
                props.put("bootstrap.servers", "localhost:9092");
                props.put("acks", "all");
                props.put("retries", 0);
                props.put("batch.size", 16384);
                props.put("linger.ms", 1);
                props.put("buffer.memory", 33554432);
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                KafkaProducer<String, String> p = new KafkaProducer<>(props);
                if (status.getGeoLocation() == null) {
                    Random random = new Random();
                    int n = random.nextInt(5);
                    t.setMsg(status.getText());
                    t.setLatitude(locationLatitude[n]);
                    t.setLongitude(locationLongitude[n]);
                    ProducerRecord<String, String> rec = new ProducerRecord<>("tweet",  gson.toJson(t), gson.toJson(t));
                    p.send(rec);
                } else {
                    t.setLatitude(status.getGeoLocation().getLatitude());
                    t.setLongitude(status.getGeoLocation().getLongitude());
                    t.setMsg(status.getText());
                    ProducerRecord<String, String> rec = new ProducerRecord<>("tweet", gson.toJson(t), gson.toJson(t));
                    p.send(rec);
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
            }
        };
        twitterStream.addListener(listener);
        FilterQuery tweetFilterQuery = new FilterQuery(); // See 
        tweetFilterQuery.track(new String[]{"#obama", "#trump"}); // OR on keywords
        twitterStream.filter(tweetFilterQuery);
        //twitterStream.sample();
    }

}