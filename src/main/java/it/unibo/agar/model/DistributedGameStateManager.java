package it.unibo.agar.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rabbitmq.client.*;
import it.unibo.agar.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DistributedGameStateManager implements GameStateManager{
    private static Logger LOGGER = LoggerFactory.getLogger(DistributedGameStateManager.class);
    private static final int MIN_FOOD_ON_THE_MAP = 15;
    private static final double PLAYER_SPEED = 1.0;
    private static final int HEIGHT = 1000;
    private static final int WIDTH = 1000;
    private static final int N_OF_FOOD = 20;
    private static final long WORLD_TIMEOUT_MS = 1000;
    private static final long PLAYER_TIMEOUT_MS = 3000;
    public static final int WINNING_MASS = 1000;
    public static final int FOOD_MASS = 150;

    private final String playerName;
    private World world;
    private final Map<String, Position> playerDirections;
    private final Serializer serializer;
    private final ElectionNode electionNode;
    private int firstTurn = 0;
    private long lastWorldMessageTimestamp = System.currentTimeMillis();
    private final Map<String, Long> lastPlayerPositionTimestamp;
    private final RabbitMQConnector connector;
    private final Boolean debug;

    public DistributedGameStateManager(String hostAddress, String playerName, Boolean debug) throws IOException,
            TimeoutException, ExecutionException, InterruptedException {
        this.debug = debug;
        lastPlayerPositionTimestamp = new HashMap<>();
        this.connector = new RabbitMQConnector();
        this.serializer = new Serializer();
        this.world = new World(WIDTH, HEIGHT, List.of(new Player(playerName,200,200,200)),
                GameInitializer.initialFoods(N_OF_FOOD, WIDTH, HEIGHT, FOOD_MASS));
        this.connector.connect(hostAddress);
        this.electionNode = new ElectionNode(playerName, this.connector, false);
        this.connector.setPlayerMessageCallback(this.updatePlayerMessageCallback());
        this.connector.setWorldMessageCallback(this.updateWorldMessageCallback());
        this.connector.setVictoryMessageCallback(this.victoryMessageCallback());
        Future<Boolean> fut = electionNode.startElection();
        if (debug)
            LOGGER.info("[{}] AM I THE LEADER -> {}", playerName, fut.get());
        this.playerName = playerName;
        this.playerDirections = new HashMap<>();
        this.world.getPlayers().forEach(p -> playerDirections.put(p.getId(), Position.ZERO));
    }

    public DeliverCallback victoryMessageCallback() {
        return (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            if (debug)
                LOGGER.info("[{}] I WIN", message);
            Main.onVictory(message);
            this.connector.victoryChannelAck(delivery);
        };
    }

    public DeliverCallback updateWorldMessageCallback() {
        return (consumerTag, delivery) -> {
            lastWorldMessageTimestamp = System.currentTimeMillis();
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            if (debug)
                LOGGER.info("[{}] RECEIVED WORLD MESSAGE -> {}", playerName, message);
            if (firstTurn <= 100) {
                firstTurn++;
            }
            try {
                if (firstTurn > 100) {
                    this.world = serializer.deserializeWorld(message);
                } else {
                    World newWorld = serializer.deserializeWorld(message);
                    this.world = new World(WIDTH, HEIGHT, this.world.getPlayers(),
                            newWorld.getFoods());
                }
            } catch (JsonProcessingException e) {
                if (debug)
                    LOGGER.error("[{}] ERROR -> {}", playerName, e.getMessage());
            }
            this.connector.worldChannelAck(delivery);
        };
    }

    public DeliverCallback updatePlayerMessageCallback() {
        return (consumerTag, delivery) -> {
            try {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                if (debug)
                    LOGGER.info("[{}] RECEIVED PLAYER MESSAGE -> {}", playerName, message);
                Player player = serializer.deserializePlayer(message);
                this.lastPlayerPositionTimestamp.put(player.getId(), System.currentTimeMillis());
                if (firstTurn <= 100 || electionNode.isLeader()) {
                    if (!player.getId().equals(this.playerName)) {
                        this.world = updatePlayerPosition(player);
                    }
                }
            } catch (JsonProcessingException e) {
                if (debug)
                    LOGGER.error("[{}] ERROR -> {}", playerName, e.getMessage());
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
        if (checkIfLeaderIsDeath()) {
            Future<Boolean> fut = this.electionNode.startElection();
            if (debug)
                LOGGER.info("[{}] AM I THE LEADER -> {}", playerName, fut.get());
            this.lastWorldMessageTimestamp = System.currentTimeMillis();
        }
        if (player.isPresent()) {
            message = serializer.serializeObject(player.get());
        } else {
            message = "";
        }
        this.connector.publishPlayerMessage(message);
        if (electionNode.isLeader()) {
            this.lastPlayerPositionTimestamp.forEach(this::removeInactivePlayers);
            this.world = this.handleEating(this.world);
            this.world = checkIfThereIsEnoughFood(this.world);
            String worldMessage = serializer.serializeObject(this.world);
            this.connector.publishWorldMessage(worldMessage);
            if (isThereAWinner()) {
                this.connector.publishVictoryMessage(getWinnerName());
            }
        }
    }

    private String getWinnerName() {
        return this.world.getPlayers().stream().filter(p -> p.getMass() >= WINNING_MASS)
                .toList().getFirst().getId();
    }

    private boolean isThereAWinner() {
        return this.world.getPlayers().stream().anyMatch(p -> p.getMass() >= WINNING_MASS);
    }

    private World checkIfThereIsEnoughFood(World world) {
        if (world.getFoods().size() < MIN_FOOD_ON_THE_MAP) {
            return new World(world.getWidth(),
                    world.getHeight(),
                    world.getPlayers(),
                    Stream.concat(world.getFoods().stream(),
                            GameInitializer.initialFoods(5, WIDTH, HEIGHT, FOOD_MASS)
                                    .stream()).toList());
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

    private boolean checkIfLeaderIsDeath() {
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
