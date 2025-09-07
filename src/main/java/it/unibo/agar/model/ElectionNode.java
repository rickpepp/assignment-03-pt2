package it.unibo.agar.model;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class ElectionNode {
    private String nodeId = null;
    private volatile String coordinatorId = null;
    private final AtomicLong lastElectionTimestamp = new AtomicLong(0);
    private final RabbitMQConnector connector;
    private final boolean debug;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "bully-node-" + nodeId);
        t.setDaemon(true);
        return t;
    });

    private final Consumer<String> coordinatorListener = id -> { };
    private volatile boolean started = false;

    public ElectionNode(String nodeId, RabbitMQConnector connector, boolean debug) throws IOException {
        this.nodeId = nodeId;
        this.debug = debug;
        this.connector = connector;
        this.start();
    }

    public ElectionNode(String nodeId, RabbitMQConnector connector) throws IOException {
        this(nodeId, connector, false);
    }

    public synchronized void start() throws IOException {
        if (started) return;

        this.connector.setElectionMessageCallback(getDeliverCallback());
        started = true;
    }

    private DeliverCallback getDeliverCallback() {
        return (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            if (this.debug)
                System.out.println("[" + nodeId + "] RAW RECEIVE -> " + message);
            try {
                handleMessage(message);
            } finally {
                try {
                    this.connector.electionChannelAck(delivery);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private void sendMessage(String type, String senderId) throws IOException {
        String body = type + "|" + senderId + "|" + System.currentTimeMillis();
        this.connector.publishElectionMessage(body);
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

    public Future<Boolean> startElection() {
        if (this.debug)
            System.out.println("[" + nodeId + "] External trigger -> startElection()");
        return executor.submit(this::startElectionInternal);
    }

    private boolean startElectionInternal() {
        try {

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
                sendMessage("COORDINATOR", this.nodeId);
                coordinatorId = this.nodeId;
                coordinatorListener.accept(this.nodeId);
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean isLeader() {
        return Objects.equals(this.nodeId, this.coordinatorId);
    }

}
