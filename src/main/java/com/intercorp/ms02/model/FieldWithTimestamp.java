package com.intercorp.ms02.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.ZonedDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FieldWithTimestamp {
    private String value;
    private ZonedDateTime timestamp;
}
