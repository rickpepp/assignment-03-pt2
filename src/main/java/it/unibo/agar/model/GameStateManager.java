package it.unibo.agar.model;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public interface GameStateManager {
    World getWorld();
    void setPlayerDirection(final String playerId, final double dx, final double dy);
    void tick() throws IOException;
}
