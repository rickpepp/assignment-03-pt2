package it.unibo.agar.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class DistributedGameStateManager implements GameStateManager{
    private static final double PLAYER_SPEED = 1.0;

    private static final String EXCHANGE_NAME = "PlayerPosition";
    private String playerName;
    private World world;
    private Channel channel;
    private final Map<String, Position> playerDirections;
    private ObjectMapper mapper;
    private int counter = 0;

    public DistributedGameStateManager(String hostAddress, String playerName) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setRequestedHeartbeat(5);
        factory.setHost(hostAddress);
        Connection connection = factory.newConnection();
        this.channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        this.world = new World(2000, 2000, List.of(new Player(playerName,200,200,200)), List.of(

        ));
        this.playerName = playerName;

        this.playerDirections = new HashMap<>();
        this.world.getPlayers().forEach(p -> playerDirections.put(p.getId(), Position.ZERO));

        mapper = new ObjectMapper();
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");


        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            try {
                Player player = mapper.readValue(message, Player.class);
                this.world = updatePlayerPosition(player);
            } catch (JsonProcessingException e) {
                ;
            }
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
        };
        channel.basicQos(1, false);
        channel.basicConsume(queueName, false, deliverCallback, consumerTag -> { });
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
        channel.basicPublish(EXCHANGE_NAME, "", new AMQP.BasicProperties.Builder().deliveryMode(2).build(), message.getBytes("UTF-8"));
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
        if (!this.world.getPlayers().stream().anyMatch(p -> Objects.equals(p.getId(), newPlayer.getId()))) {
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
