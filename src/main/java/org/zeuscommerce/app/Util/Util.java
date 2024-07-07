package org.zeuscommerce.app.Util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.zeuscommerce.app.Entity.Order;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Util {

    public static OrderMsg Json2Order(String jsonString) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(jsonString.trim().replaceFirst("\ufeff", ""));
        return objectMapper.treeToValue(jsonNode, OrderMsg.class);
    }


    public static String formatDateTime(LocalDateTime dateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return dateTime.format(formatter);
    }
}
