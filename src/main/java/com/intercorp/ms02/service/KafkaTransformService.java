package com.intercorp.ms02.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.intercorp.ms02.model.FieldWithTimestamp;
import com.intercorp.ms02.model.KafkaMessageWrapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class KafkaTransformService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @KafkaListener(topics = "kafka-topic-01", groupId = "ms02-group")
    public void listen(String message) {
        try {
            System.out.println("MS02 recibió mensaje: " + message);

            KafkaMessageWrapper wrapper = objectMapper.readValue(message, KafkaMessageWrapper.class);

            Map<String, Object> rawFields = objectMapper.convertValue(wrapper, new TypeReference<>() {});
            Map<String, FieldWithTimestamp> enriched = new HashMap<>();

            ZonedDateTime now = ZonedDateTime.now();
            rawFields.forEach((key, value) -> {
                enriched.put(key, new FieldWithTimestamp(String.valueOf(value), now));
            });

            String outputJson = objectMapper.writeValueAsString(enriched);
            System.out.println("Mensaje transformado y enviado: " + outputJson);

            kafkaTemplate.send("kafka-topic-02", outputJson);
        } catch (Exception e) {
            System.err.println("Error en KafkaTransformService: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
