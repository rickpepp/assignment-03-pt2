package it.unibo.agar.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record ElectionMessage(String type, String senderId, long timestamp) {
    @JsonCreator
    public ElectionMessage(
            @JsonProperty("type") String type,
            @JsonProperty("senderId") String senderId,
            @JsonProperty("timestamp") long timestamp) {
        this.type = type;
        this.senderId = senderId;
        this.timestamp = timestamp;
    }
}
