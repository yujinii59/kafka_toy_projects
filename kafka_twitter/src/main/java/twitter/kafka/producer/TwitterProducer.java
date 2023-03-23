package twitter.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TwitterProducer {

    public Producer<String, String> producer;

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

        producer = new KafkaProducer<>(props);

        producer.initTransactions();

        producer.beginTransaction();
    }

    public void putDataToKafka(String data) {

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                                                                                    "twitter-chatgpt",
                                                                                    data
                                                                            );
            producer.send(record);
            producer.flush();
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
