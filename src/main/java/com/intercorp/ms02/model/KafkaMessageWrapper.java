package com.intercorp.ms02.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class KafkaMessageWrapper {
    private Person person;

    private int random;

    @JsonProperty("randomFloat")
    private double randomFloat;

    private boolean bool;
    private String date;

    @JsonProperty("regEx")
    private String regex;

    @JsonProperty("enumValue")
    private String enumValue;

    private List<String> elements;

    private int age;
}
