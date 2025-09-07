package it.unibo.agar.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class RabbitMQConnector {
    private static final String EXCHANGE_NAME_PLAYER_POSITION = "PlayerPosition";
    private static final String EXCHANGE_NAME_ACTUAL_WORLD = "ActualWorld";
    private static final String EXCHANGE_NAME_ELECTION = "Election";
    private static final String EXCHANGE_NAME_VICTORY = "Victory";
    private Channel playerChannel;
    private Channel worldChannel;
    private Channel electionChannel;
    private Channel victoryChannel;

    private String playerQueueName;
    private String worldQueueName;
    private String electionQueueName;
    private String victoryQueueName;

    public void connect(String hostAddress) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(hostAddress);
        Connection connection = factory.newConnection();
        setElectionChannel(connection);
        setPlayerChannel(connection);
        setWorldChannel(connection);
        setVictoryChannel(connection);
    }

    private void setElectionChannel(Connection connection) throws IOException {
        this.electionChannel = connection.createChannel();
        electionChannel.exchangeDeclare(EXCHANGE_NAME_ELECTION, BuiltinExchangeType.FANOUT, true);
        electionQueueName = electionChannel.queueDeclare("", false, true, true, null).getQueue();
        electionChannel.queueBind(electionQueueName, EXCHANGE_NAME_ELECTION, "");
        electionChannel.basicQos(1, false);
    }

    private void setWorldChannel(Connection connection) throws IOException {
        this.worldChannel = connection.createChannel();
        worldChannel.exchangeDeclare(EXCHANGE_NAME_ACTUAL_WORLD, "fanout");
        worldQueueName = worldChannel.queueDeclare().getQueue();
        worldChannel.queueBind(worldQueueName, EXCHANGE_NAME_ACTUAL_WORLD, "");
        worldChannel.basicQos(1, false);
    }

    private void setPlayerChannel(Connection connection) throws IOException {
        this.playerChannel = connection.createChannel();
        playerChannel.exchangeDeclare(EXCHANGE_NAME_PLAYER_POSITION, "fanout");
        playerQueueName = playerChannel.queueDeclare().getQueue();
        playerChannel.queueBind(playerQueueName, EXCHANGE_NAME_PLAYER_POSITION, "");
        playerChannel.basicQos(1, false);
    }

    private void setVictoryChannel(Connection connection) throws IOException {
        this.victoryChannel = connection.createChannel();
        victoryChannel.exchangeDeclare(EXCHANGE_NAME_VICTORY, "fanout");
        victoryQueueName = victoryChannel.queueDeclare().getQueue();
        victoryChannel.queueBind(victoryQueueName, EXCHANGE_NAME_VICTORY, "");
        victoryChannel.basicQos(1, false);
    }

    public void setElectionMessageCallback(DeliverCallback callback) throws IOException {
        electionChannel.basicConsume(electionQueueName, false, callback, consumerTag -> { });
    }

    public void setPlayerMessageCallback(DeliverCallback callback) throws IOException {
        playerChannel.basicConsume(playerQueueName, false, callback, consumerTag -> { });
    }

    public void setWorldMessageCallback(DeliverCallback callback) throws IOException {
        worldChannel.basicConsume(worldQueueName, false, callback, consumerTag -> { });
    }

    public void setVictoryMessageCallback(DeliverCallback callback) throws IOException {
        victoryChannel.basicConsume(victoryQueueName, false, callback, consumerTag -> { });
    }

    public void publishPlayerMessage(String message) throws IOException {
        playerChannel.basicPublish(EXCHANGE_NAME_PLAYER_POSITION, "", new AMQP.BasicProperties.Builder().deliveryMode(2).build(),
                message.getBytes(StandardCharsets.UTF_8));
    }

    public void publishWorldMessage(String message) throws IOException {
        worldChannel.basicPublish(EXCHANGE_NAME_ACTUAL_WORLD, "", new AMQP.BasicProperties.Builder().deliveryMode(2).build(),
                message.getBytes(StandardCharsets.UTF_8));
    }

    public void publishElectionMessage(String message) throws IOException {
        electionChannel.basicPublish(EXCHANGE_NAME_ELECTION, "", new AMQP.BasicProperties.Builder().deliveryMode(2).build(),
                message.getBytes(StandardCharsets.UTF_8));
    }

    public void publishVictoryMessage(String message) throws IOException {
        victoryChannel.basicPublish(EXCHANGE_NAME_VICTORY, "", new AMQP.BasicProperties.Builder().deliveryMode(2).build(),
                message.getBytes(StandardCharsets.UTF_8));
    }

    public void worldChannelAck(Delivery delivery) throws IOException {
        worldChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
    }

    public void playerChannelAck(Delivery delivery) throws IOException {
        playerChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
    }

    public void electionChannelAck(Delivery delivery) throws IOException {
        electionChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
    }

    public void victoryChannelAck(Delivery delivery) throws IOException {
        victoryChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
    }
}
