package it.unibo.agar.model;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class BullyNodeExchange {

    private static final String EXCHANGE_NAME = "bully_exchange";

    private String nodeId = null;
    private final ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private String queueName;
    private volatile String coordinatorId = null;
    private final AtomicLong lastElectionTimestamp = new AtomicLong(0);

    private boolean debug;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "bully-node-" + nodeId);
        t.setDaemon(true);
        return t;
    });

    // callback for coordinator changes
    private Consumer<String> coordinatorListener = id -> { };

    private volatile boolean started = false;

    public BullyNodeExchange(String nodeId, String rabbitHost, boolean debug) {
        this.nodeId = nodeId;
        factory = new ConnectionFactory();
        factory.setHost(rabbitHost);
    }

    public BullyNodeExchange(String nodeId, String rabbitHost) {
        this(nodeId, rabbitHost, false);
    }

    // set a listener to be notified when coordinator changes
    public void setCoordinatorListener(Consumer<String> listener) {
        if (listener != null) this.coordinatorListener = listener;
    }

    // Connect to RabbitMQ and prepare exchange/queue but do not start listening
    public synchronized void connect() throws IOException, TimeoutException {
        if (connection != null && connection.isOpen()) return;
        if (this.debug)
            System.out.println("[" + nodeId + "] Connecting to RabbitMQ...");
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT, true);
        queueName = channel.queueDeclare("", false, true, true, null).getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");
        if (this.debug)
            System.out.println("[" + nodeId + "] Connected. queue=" + queueName + " exchange=" + EXCHANGE_NAME);
    }

    // Start consuming messages; idempotent
    public synchronized void start() throws IOException {
        if (started) return;
        if (channel == null || !channel.isOpen()) {
            throw new IllegalStateException("Not connected. Call connect() first.");
        }

        if (this.debug)
            System.out.println("[" + nodeId + "] Starting consumer on queue " + queueName);
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            if (this.debug)
                System.out.println("[" + nodeId + "] RAW RECEIVE -> " + message);
            try {
                handleMessage(message);
            } finally {
                try {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        channel.basicConsume(queueName, false, deliverCallback, consumerTag -> { });
        started = true;
    }

    // Disconnect and stop consuming
    public synchronized void stop() throws IOException, TimeoutException {
        if (this.debug)
            System.out.println("[" + nodeId + "] Stopping node...");
        started = false;
        executor.shutdownNow();
        if (channel != null && channel.isOpen()) channel.close();
        if (connection != null && connection.isOpen()) connection.close();
        channel = null;
        connection = null;
        if (this.debug)
            System.out.println("[" + nodeId + "] Stopped.");
    }

    // Messaggi: TYPE|SENDER_ID|TIMESTAMP
    private void sendMessage(String type, String senderId) throws IOException {
        String body = type + "|" + senderId + "|" + System.currentTimeMillis();
        channel.basicPublish(EXCHANGE_NAME, "", null, body.getBytes(StandardCharsets.UTF_8));
        if (this.debug)
            System.out.println("[" + nodeId + "] SEND -> " + body);
    }

    private void handleMessage(String msg) throws IOException {
        String[] parts = msg.split("\\|");
        if (parts.length < 2) {
            System.out.println("[" + nodeId + "] Malformed msg: " + msg);
            return;
        }
        String type = parts[0];
        String sender = parts[1];

        if (Objects.equals(sender, this.nodeId)) {
            if (this.debug)
                System.out.println("[" + nodeId + "] Ignoring my own message");
            return;
        }

        if (this.debug)
            System.out.println("[" + nodeId + "] HANDLE -> type=" + type + " from=" + sender);

        switch (type) {
            case "ELECTION":
                this.coordinatorId = null;
                if (this.debug)
                    System.out.println("[" + nodeId + "] Received ELECTION from " + sender +
                            " (myId=" + nodeId + ", compare=" + nodeId.compareTo(sender) + ")");
                if (this.nodeId.compareTo(sender) > 0) {
                    if (this.debug)
                        System.out.println("[" + nodeId + "] I am higher -> send OK and start own election");
                    sendMessage("OK", this.nodeId);
                    // start own election asynchronously but don't block message handling
                    executor.submit(this::startElectionInternal);
                } else {
                    if (this.debug)
                        System.out.println("[" + nodeId + "] I am lower -> do not respond with OK");
                }
                break;

            case "OK":
                if (this.debug)
                    System.out.println("[" + nodeId + "] Received OK from " + sender);
                lastElectionTimestamp.set(System.currentTimeMillis());
                break;

            case "COORDINATOR":
                if (this.debug)
                    System.out.println("[" + nodeId + "] Received COORDINATOR from " + sender);
                coordinatorId = sender;
                coordinatorListener.accept(sender);
                break;

            default:
                if (this.debug)
                    System.out.println("[" + nodeId + "] Unknown msg: " + msg);
        }
    }

    // Public method to trigger an election on demand. Returns a Future that completes with true
    // if this node became coordinator, false otherwise.
    public Future<Boolean> startElection() {
        if (this.debug)
            System.out.println("[" + nodeId + "] External trigger -> startElection()");
        return executor.submit(() -> startElectionInternal());
    }

    // internal election logic (returns true when this node became coordinator)
    private boolean startElectionInternal() {
        try {
            if (channel == null || !channel.isOpen()) {
                throw new IllegalStateException("Not connected/started");
            }

            long sendTs = System.currentTimeMillis();
            if (this.debug)
                System.out.println("[" + nodeId + "] Starting election... ts=" + sendTs);
            sendMessage("ELECTION", this.nodeId);

            int waitMs = 600;
            int interval = 200;
            boolean gotOk = false;

            long startTs = System.currentTimeMillis();
            lastElectionTimestamp.set(startTs);

            long waited = 0;
            while (waited < waitMs) {
                Thread.sleep(interval);
                waited += interval;
                long last = lastElectionTimestamp.get();
                if (this.debug)
                    System.out.println("[" + nodeId + "] Waiting... waited=" + waited + " lastOkTs=" + last + " startTs=" + startTs);
                if (last > startTs) {
                    gotOk = true;
                    break;
                }
            }

            if (!gotOk) {
                //System.out.println("[" + nodeId + "] No OK received -> I become coordinator");
                sendMessage("COORDINATOR", this.nodeId);
                coordinatorId = this.nodeId;
                coordinatorListener.accept(this.nodeId);
                return true;
            } else {
                //System.out.println("[" + nodeId + "] OK received -> waiting for COORDINATOR announcement");
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public String getCoordinatorId() {
        return coordinatorId;
    }

    public boolean isLeader() {
        return Objects.equals(this.nodeId, this.coordinatorId);
    }

}
