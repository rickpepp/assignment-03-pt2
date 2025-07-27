package it.unibo.agar.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Player extends AbstractEntity {
    @JsonCreator
    public Player(
            @JsonProperty("id") final String id,
            @JsonProperty("x") final double x,
            @JsonProperty("y") final double y,
            @JsonProperty("mass") final double mass) {
        super(id, x, y, mass);
    }


    public Player grow(Entity entity) {
        return new Player(getId(), getX(), getY(), getMass() + entity.getMass());
    }

    public Player moveTo(double newX, double newY) {
        return new Player(getId(), newX, newY, getMass());
    }
}
