package it.unibo.agar.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Serializer {
    private final ObjectMapper mapper;

    public Serializer() {
        mapper = new ObjectMapper();
    }

    public String serializeObject(Object object) throws JsonProcessingException {
        return mapper.writeValueAsString(object);
    }

    public World deserializeWorld(String message) throws JsonProcessingException {
        return mapper.readValue(message, World.class);
    }

    public Player deserializePlayer(String message) throws JsonProcessingException {
        return mapper.readValue(message, Player.class);
    }

    public ElectionMessage deserializeElectionMessage(String message) throws JsonProcessingException {
        return mapper.readValue(message, ElectionMessage.class);
    }
}
