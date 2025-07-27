package it.unibo.agar.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Food extends AbstractEntity {
    public static final double DEFAULT_MASS = 100.0;
    public Food(
            @JsonProperty("id") final String id,
            @JsonProperty("x") final double x,
            @JsonProperty("y") final double y,
            @JsonProperty("mass") final double mass) {
        super(id, x, y, mass);
    }
}
