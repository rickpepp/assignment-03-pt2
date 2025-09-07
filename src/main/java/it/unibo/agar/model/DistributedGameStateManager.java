package it.unibo.agar.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DistributedGameStateManager implements GameStateManager{
    private static final double PLAYER_SPEED = 1.0;
    private static final int HEIGHT = 1000;
    private static final int WIDTH = 1000;
    private static final int N_OF_FOOD = 20;
    private final String playerName;
    private World world;
    private final Map<String, Position> playerDirections;
    private ObjectMapper mapper;
    private BullyNodeExchange node;
    private int firstTurn = 0;
    private long lastWorldMessageTimestamp = System.currentTimeMillis();
    private final Map<String, Long> lastPlayerPositionTimestamp;
    private final long WORLD_TIMEOUT_MS = 1000;
    private final long PLAYER_TIMEOUT_MS = 3000;
    private final RabbitMQConnector connector;

    public DistributedGameStateManager(String hostAddress, String playerName) throws IOException, TimeoutException, ExecutionException, InterruptedException {
        lastPlayerPositionTimestamp = new HashMap<>();
        this.world = new World(WIDTH, HEIGHT, List.of(new Player(playerName,200,200,200)),
                GameInitializer.initialFoods(N_OF_FOOD, WIDTH, HEIGHT, 150));
        this.node = new BullyNodeExchange(playerName, hostAddress);
        node.connect();
        node.start();
        mapper = new ObjectMapper();
        this.connector = new RabbitMQConnector();
        this.connector.connect(hostAddress);
        this.connector.setPlayerMessageCallback(this.updatePlayerMessageCallback());
        this.connector.setWorldMessageCallback(this.updateWorldMessageCallback());
        Future<Boolean> fut = node.startElection();
        System.out.println("Am I the leader: " + fut.get());
        this.playerName = playerName;
        this.playerDirections = new HashMap<>();
        this.world.getPlayers().forEach(p -> playerDirections.put(p.getId(), Position.ZERO));
    }

    public DeliverCallback updateWorldMessageCallback() throws IOException {
        return (consumerTag, delivery) -> {
            lastWorldMessageTimestamp = System.currentTimeMillis();
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            if (firstTurn <= 100) {
                firstTurn++;
            }
            try {
                if (firstTurn > 100) {
                    this.world = mapper.readValue(message, World.class);
                } else {
                    World newWorld = mapper.readValue(message, World.class);
                    this.world = new World(WIDTH, HEIGHT, this.world.getPlayers(),
                            newWorld.getFoods());
                }
            } catch (JsonProcessingException ignored) {
                ;
            }
            this.connector.worldChannelAck(delivery);
        };
    }

    public DeliverCallback updatePlayerMessageCallback() throws IOException {
        return (consumerTag, delivery) -> {
            try {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                Player player = mapper.readValue(message, Player.class);
                this.lastPlayerPositionTimestamp.put(player.getId(), System.currentTimeMillis());
                if (firstTurn <= 100 || node.isLeader()) {
                    if (!player.getId().equals(this.playerName)) {
                        this.world = updatePlayerPosition(player);
                    }
                }
            } catch (JsonProcessingException ignored) {
                ;
            }
            this.connector.playerChannelAck(delivery);
        };
    }

    @Override
    public World getWorld() {
        return this.world;
    }

    @Override
    public void setPlayerDirection(String playerId, double dx, double dy) {
        if (world.getPlayerById(playerId).isPresent()) {
            this.playerDirections.put(playerId, Position.of(dx, dy));
        }
    }

    @Override
    public void tick() throws IOException, ExecutionException, InterruptedException {
        this.world = moveAllPlayers(this.world);
        Optional<Player> player = this.world.getPlayerById(this.playerName);
        String message;

        if (checkIfLeaderIsAlive()) {
            Future<Boolean> fut = this.node.startElection();
            System.out.println("New election result, Am I leader: " + fut.get());
            this.lastWorldMessageTimestamp = System.currentTimeMillis();
        }

        if (player.isPresent()) {
            message = mapper.writeValueAsString(player.get());
        } else {
            message = "";
        }
        this.connector.publishPlayerMessage(message);

        if (node.isLeader()) {
            this.lastPlayerPositionTimestamp.forEach(this::removeInactivePlayers);
            this.world = this.handleEating(this.world);
            this.world = checkIfThereIsEnoughFood(this.world);
            String worldMessage = mapper.writeValueAsString(this.world);
            this.connector.publishWorldMessage(worldMessage);
        }
    }

    private World checkIfThereIsEnoughFood(World world) {
        if (world.getFoods().size() < 15) {
            return new World(world.getWidth(),
                    world.getHeight(),
                    world.getPlayers(),
                    Stream.concat(world.getFoods().stream(), GameInitializer.initialFoods(5, WIDTH, HEIGHT, 150).stream()).toList());
        }
        return world;
    }

    private void removeInactivePlayers(String playerId, long lastTimestamp) {
        if (System.currentTimeMillis() - lastTimestamp > PLAYER_TIMEOUT_MS) {
            this.world = new World(this.world.getWidth(),
                    this.world.getHeight(),
                    this.world.getPlayers().stream().filter(p -> !p.getId().equals(playerId)).toList(),
                    this.world.getFoods());
        }
    }

    private boolean checkIfLeaderIsAlive() {
        return System.currentTimeMillis() - this.lastWorldMessageTimestamp > WORLD_TIMEOUT_MS;
    }

    private World handleEating(final World currentWorld) {
        final List<Player> updatedPlayers = currentWorld.getPlayers().stream()
                .map(player -> growPlayer(currentWorld, player))
                .toList();

        final List<Food> foodsToRemove = currentWorld.getPlayers().stream()
                .flatMap(player -> eatenFoods(currentWorld, player).stream())
                .distinct()
                .toList();

        final List<Player> playersToRemove = currentWorld.getPlayers().stream()
                .flatMap(player -> eatenPlayers(currentWorld, player).stream())
                .distinct()
                .toList();

        return new World(currentWorld.getWidth(), currentWorld.getHeight(), updatedPlayers, currentWorld.getFoods())
                .removeFoods(foodsToRemove)
                .removePlayers(playersToRemove);
    }

    private Player growPlayer(final World world, final Player player) {
        final Player afterFood = eatenFoods(world, player).stream()
                .reduce(player, Player::grow, (p1, p2) -> p1);

        return eatenPlayers(world, afterFood).stream()
                .reduce(afterFood, Player::grow, (p1, p2) -> p1);
    }

    private List<Food> eatenFoods(final World world, final Player player) {
        return world.getFoods().stream()
                .filter(food -> EatingManager.canEatFood(player, food))
                .toList();
    }

    private List<Player> eatenPlayers(final World world, final Player player) {
        return world.getPlayersExcludingSelf(player).stream()
                .filter(other -> EatingManager.canEatPlayer(player, other))
                .toList();
    }

    private World moveAllPlayers(final World currentWorld) {
        final List<Player> updatedPlayers = currentWorld.getPlayers().stream()
                .map(player -> {
                    Position direction = playerDirections.getOrDefault(player.getId(), Position.ZERO);
                    final double newX = player.getX() + direction.x() * PLAYER_SPEED;
                    final double newY = player.getY() + direction.y() * PLAYER_SPEED;
                    return player.moveTo(newX, newY);
                })
                .collect(Collectors.toList());
        return new World(currentWorld.getWidth(), currentWorld.getHeight(), updatedPlayers, currentWorld.getFoods());
    }

    private World updatePlayerPosition(Player newPlayer) {
        final List<Player> updatedPlayers;
        if (isPlayerNotPresent(newPlayer)) {
            updatedPlayers = new ArrayList<>(this.world.getPlayers());
            updatedPlayers.add(newPlayer);
        } else {
            updatedPlayers = updateExistentPlayer(newPlayer);
        }
        return new World(this.world.getWidth(), this.world.getHeight(), updatedPlayers, this.world.getFoods());
    }

    private List<Player> updateExistentPlayer(Player newPlayer) {
        return this.world.getPlayers().stream()
                .map(player -> {
                    if (player.getId().equals(newPlayer.getId())) {
                        return newPlayer;
                    } else {
                        return player;
                    }
                })
                .collect(Collectors.toList());
    }

    private boolean isPlayerNotPresent(Player newPlayer) {
        return this.world.getPlayers().stream().noneMatch(p -> Objects.equals(p.getId(), newPlayer.getId()));
    }
}
