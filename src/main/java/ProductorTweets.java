import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;

public class ProductorTweets {

    public final static String TOPIC_NAME = "rawtweets";

    public static void main(String[] args){

        String apiKey = args[0];
        String apiSecret = args[1];
        String tokenValue = args[2];
        String tokenSecret = args[3];

        Properties props = new Properties();
        props.put("acks","1");
        props.put("retries",3);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        final KafkaProducer<String, String> prod = new KafkaProducer<String, String>(props);

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthAccessToken(tokenValue);
        cb.setOAuthAccessTokenSecret(tokenSecret);
        cb.setOAuthConsumerKey(apiKey);
        cb.setOAuthConsumerSecret(apiSecret);
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);

        final TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        try{
            StatusListener listenerEx = new StatusListener() {
                @Override
                public void onStatus(Status status) {
                    HashtagEntity[] hashtags = status.getHashtagEntities();
                    if(hashtags.length > 0){
                        String value = TwitterObjectFactory.getRawJSON(status);
                        String lang = status.getLang();
                        prod.send(new ProducerRecord<>(ProductorTweets.TOPIC_NAME, lang, value));
                    }
                }

                @Override
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

                @Override
                public void onTrackLimitationNotice(int i) {}

                @Override
                public void onScrubGeo(long l, long l1) {}

                @Override
                public void onStallWarning(StallWarning stallWarning) {}

                @Override
                public void onException(Exception e) {}
            };
            twitterStream.addListener(listenerEx);
            twitterStream.sample();
        }catch (Exception e){
            e.printStackTrace();
            prod.close();
        }
    }
}
