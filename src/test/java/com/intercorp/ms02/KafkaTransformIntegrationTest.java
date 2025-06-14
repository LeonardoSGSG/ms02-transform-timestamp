package com.intercorp.ms02;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = { "kafka-topic-01", "kafka-topic-02" })
@EnableKafka
public class KafkaTransformIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void testMessageTransformationAndForwarding() throws Exception {
        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(senderProps, new StringSerializer(), new StringSerializer());
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);

        String inputJson = """
            {
              "person": {
                "firstname": "Nonnah",
                "lastname": "Waite",
                "firstname2": "Myriam",
                "lastname2": "Kronfeld",
                "city": "Minsk",
                "country": "France",
                "email": "Myriam.Kronfeld@yopmail.com",
                "random": 22,
                "randomFloat": 17.059,
                "bool": true,
                "date": "1998-12-11",
                "regEx": "hellooooooooooooooooooooooo to you",
                "enumValue": "generator",
                "elements": ["Jaime", "Kimberley", "Caryl", "Basia", "Thalia"],
                "age": 56
              },
              "random": 22,
              "randomFloat": 17.059,
              "bool": true,
              "date": "1998-12-11",
              "regEx": "hellooooooooooooooooooooooo to you",
              "enumValue": "generator",
              "elements": ["Jaime", "Kimberley", "Caryl", "Basia", "Thalia"],
              "age": 56
            }
        """;

        kafkaTemplate.send("kafka-topic-01", inputJson);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(
                consumerProps,
                new StringDeserializer(),
                new StringDeserializer()
        );

        Consumer<String, String> consumer = consumerFactory.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "kafka-topic-02");


        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, "kafka-topic-02");
        String transformedJson = record.value();

        System.out.println("Mensaje transformado recibido en test: " + transformedJson);

        assertThat(transformedJson).contains("value");
        assertThat(transformedJson).contains("timestamp");
        assertThat(transformedJson).contains("firstname");
        assertThat(transformedJson).contains("Nonnah");
    }
}
