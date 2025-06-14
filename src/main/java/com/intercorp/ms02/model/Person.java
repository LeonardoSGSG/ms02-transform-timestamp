package com.intercorp.ms02.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class Person {

    @JsonProperty("firstname")
    private String firstname;

    @JsonProperty("lastname")
    private String lastname;

    @JsonProperty("firstname2")
    private String firstname2;

    @JsonProperty("lastname2")
    private String lastname2;

    @JsonProperty("city")
    private String city;

    @JsonProperty("country")
    private String country;

    @JsonProperty("email")
    private String email;

    @JsonProperty("random")
    private int random;

    @JsonProperty("randomFloat")
    private double randomFloat;

    @JsonProperty("bool")
    private boolean bool;

    @JsonProperty("date")
    private String date;

    @JsonProperty("regEx")
    private String regex;

    @JsonProperty("enumValue")
    private String enumValue;

    @JsonProperty("elements")
    private List<String> elements;

    @JsonProperty("age")
    private int age;
}
