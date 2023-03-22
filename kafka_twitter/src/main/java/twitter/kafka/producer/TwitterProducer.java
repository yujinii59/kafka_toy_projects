package twitter.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.util.Properties;

public class TwitterProducer {

    public Producer<String, JSONObject> producer;


    public TwitterProducer() {

        String bootstrapServers = "kafka1:9092,kafka2:9092,kafka3:9092";
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "5");

        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "twitter-transaction-chatgpt");

        this.producer = new KafkaProducer<>(props);

        this.producer.initTransactions();

        this.producer.beginTransaction();
    }

    public void putDataToKafka(JSONObject data) {

        try {
            ProducerRecord<String, JSONObject> record = new ProducerRecord<>(
                                                                                    "twitter-chatgpt",
                                                                                    data
                                                                            );
            this.producer.send(record);
            this.producer.flush();
            System.out.println();
        } catch (Exception e) {
            producer.abortTransaction(); // 프로듀서 트랜잭션 중단
            e.printStackTrace();
        } finally {
            producer.commitTransaction(); // 프로듀서 트랜잭션 커밋
            producer.close();
        }

    }
}
