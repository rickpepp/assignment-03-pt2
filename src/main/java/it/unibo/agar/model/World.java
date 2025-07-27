package it.unibo.agar.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class World {
    private final int width;
    private final int height;
    private final List<Player> players;
    private final List<Food> foods;

    @JsonCreator
    public World(
            @JsonProperty("width") int width,
            @JsonProperty("height") int height,
            @JsonProperty("players") List<Player> players,
            @JsonProperty("foods") List<Food> foods) {
        this.width = width;
        this.height = height;
        this.players = List.copyOf(players); // Ensure immutability
        this.foods = List.copyOf(foods);     // Ensure immutability
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public List<Player> getPlayers() {
        return players;
    }

    public List<Food> getFoods() {
        return foods;
    }

    public List<Player> getPlayersExcludingSelf(final Player player) {
        return players.stream()
                .filter(p -> !p.getId().equals(player.getId()))
                .collect(Collectors.toList());
    }

    public Optional<Player> getPlayerById(final String id) {
        return players.stream()
                .filter(p -> p.getId().equals(id))
                .findFirst();
    }


    public World removePlayers(final List<Player> playersToRemove) {
        List<String> idsToRemove = playersToRemove.stream().map(Player::getId).toList();
        List<Player> newPlayers = players.stream()
                .filter(p -> !idsToRemove.contains(p.getId()))
                .collect(Collectors.toList());
        return new World(width, height, newPlayers, foods);
    }

    public World removeFoods(List<Food> foodsToRemove) {
        List<Food> newFoods = foods.stream()
                .filter(f -> !foodsToRemove.contains(f)) // Assumes Food has proper equals/hashCode or relies on object identity if not overridden
                .collect(Collectors.toList());
        return new World(width, height, players, newFoods);
    }
}
