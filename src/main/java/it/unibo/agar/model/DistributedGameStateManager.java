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

public class DistributedGameStateManager implements GameStateManager{
    private static final double PLAYER_SPEED = 1.0;
    private static final int HEIGHT = 1000;
    private static final int WIDTH = 1000;
    private static final int N_OF_FOOD = 20;

    private static final String EXCHANGE_NAME_PLAYER_POSITION = "PlayerPosition";
    private static final String EXCHANGE_NAME_ACTUAL_WORLD = "ActualWorld";
    private final String playerName;
    private World world;
    private Channel playerChannel;
    private Channel worldChannel;
    private final Map<String, Position> playerDirections;
    private ObjectMapper mapper;

    private BullyNodeExchange node;

    private int firstTurn = 0;

    public DistributedGameStateManager(String hostAddress, String playerName) throws IOException, TimeoutException, ExecutionException, InterruptedException {
        this.world = new World(WIDTH, HEIGHT, List.of(new Player(playerName,200,200,200)),
                GameInitializer.initialFoods(N_OF_FOOD, WIDTH, HEIGHT, 150));
        this.node = new BullyNodeExchange(playerName, hostAddress);
        node.connect();
        node.start();
        this.setRabbitMQConnection(hostAddress);
        
        Future<Boolean> fut = node.startElection();
        System.out.println("Am I the leader: " + fut.get());
        this.playerName = playerName;
        this.playerDirections = new HashMap<>();
        this.world.getPlayers().forEach(p -> playerDirections.put(p.getId(), Position.ZERO));
    }

    private void setRabbitMQConnection(String hostAddress) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(hostAddress);
        Connection connection = factory.newConnection();
        this.playerChannel = connection.createChannel();
        playerChannel.exchangeDeclare(EXCHANGE_NAME_PLAYER_POSITION, "fanout");
        mapper = new ObjectMapper();
        String queueName = playerChannel.queueDeclare().getQueue();
        playerChannel.queueBind(queueName, EXCHANGE_NAME_PLAYER_POSITION, "");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            if (firstTurn <= 100 || node.isLeader()) {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                try {
                    Player player = mapper.readValue(message, Player.class);
                    if (!player.getId().equals(this.playerName)) {
                        this.world = updatePlayerPosition(player);
                    }
                } catch (JsonProcessingException ignored) {
                    ;
                }
            }
            
            playerChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
        };
        playerChannel.basicQos(1, false);
        playerChannel.basicConsume(queueName, false, deliverCallback, consumerTag -> { });

        this.worldChannel = connection.createChannel();
        worldChannel.exchangeDeclare(EXCHANGE_NAME_ACTUAL_WORLD, "fanout");
        String queueNameWorld = worldChannel.queueDeclare().getQueue();
        worldChannel.queueBind(queueNameWorld, EXCHANGE_NAME_ACTUAL_WORLD, "");
        DeliverCallback deliverCallbackWorld = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            if (firstTurn <= 100) {
                firstTurn++;
            }
            try {
                if (firstTurn > 100) {
                    World newWorld = mapper.readValue(message, World.class);
                    this.world = newWorld;
                } else {
                    World newWorld = mapper.readValue(message, World.class);
                    this.world = new World(WIDTH, HEIGHT, this.world.getPlayers(),
                        newWorld.getFoods());
                }
            } catch (JsonProcessingException ignored) {
                ; 
            }
            worldChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
        };
        worldChannel.basicQos(1, false);
        worldChannel.basicConsume(queueNameWorld, false, deliverCallbackWorld, consumerTag -> { });
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
    public void tick() throws IOException {
        this.world = moveAllPlayers(this.world);
        Optional<Player> player = this.world.getPlayerById(this.playerName);
        String message;
        if (player.isPresent()) {
            message = mapper.writeValueAsString(player.get());
        } else {
            message = "";
        }
        playerChannel.basicPublish(EXCHANGE_NAME_PLAYER_POSITION, "", new AMQP.BasicProperties.Builder().deliveryMode(2).build(),
                message.getBytes(StandardCharsets.UTF_8));

        if (node.isLeader()) {
            this.world = this.handleEating(this.world);
            String worldMessage = mapper.writeValueAsString(this.world);
            worldChannel.basicPublish(EXCHANGE_NAME_ACTUAL_WORLD, "", new AMQP.BasicProperties.Builder().deliveryMode(2).build(),
                    worldMessage.getBytes(StandardCharsets.UTF_8));   
        }
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
        if (this.world.getPlayers().stream().noneMatch(p -> Objects.equals(p.getId(), newPlayer.getId()))) {
            updatedPlayers = new ArrayList<>(this.world.getPlayers());
            updatedPlayers.add(newPlayer);
        } else {
            updatedPlayers = this.world.getPlayers().stream()
                    .map(player -> {
                        if (player.getId().equals(newPlayer.getId())) {
                            return newPlayer;
                        } else {
                            return player;
                        }
                    })
                    .collect(Collectors.toList());
        }
        return new World(this.world.getWidth(), this.world.getHeight(), updatedPlayers, this.world.getFoods());
    }
}
