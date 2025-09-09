package it.unibo.agar.model;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class ElectionNode {
    private static Logger LOGGER = LoggerFactory.getLogger(ElectionNode.class);
    private String nodeId = null;
    private volatile String coordinatorId = null;
    private final AtomicLong lastElectionTimestamp = new AtomicLong(0);
    private final RabbitMQConnector connector;
    private final boolean debug;
    private final Serializer serializer;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "election-node-" + nodeId);
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
        this.serializer = new Serializer();
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
                LOGGER.info("[{}] RAW RECEIVE -> {}", nodeId, message);
            try {
                handleMessage(message);
            } finally {
                try {
                    this.connector.electionChannelAck(delivery);
                } catch (IOException e) {
                    LOGGER.error(e.getMessage());
                }
            }
        };
    }

    private void sendMessage(String type, String senderId) throws IOException {
        String body = this.serializer.serializeObject(
                new ElectionMessage(type, senderId, System.currentTimeMillis()));
        this.connector.publishElectionMessage(body);
        if (this.debug)
            LOGGER.info("[{}] SEND -> {}", nodeId, body);
    }

    private void handleMessage(String msg) throws IOException {
        ElectionMessage message = this.serializer.deserializeElectionMessage(msg);
        String type = message.type();
        String sender = message.senderId();

        if (Objects.equals(sender, this.nodeId)) {
            if (this.debug)
                LOGGER.info("[{}] Ignoring my own message", nodeId);
            return;
        }

        if (this.debug)
            LOGGER.info("[{}] HANDLE -> type={} from={}", nodeId, type, sender);

        switch (type) {
            case "ELECTION":
                this.coordinatorId = null;
                if (this.debug)
                    LOGGER.info("[{}] Received ELECTION from {} (myId={}, compare={})", nodeId, sender, nodeId, nodeId.compareTo(sender));
                if (this.nodeId.compareTo(sender) > 0) {
                    if (this.debug)
                        LOGGER.info("[{}] I am higher -> send OK and start own election", nodeId);
                    sendMessage("OK", this.nodeId);
                    executor.submit(this::startElectionInternal);
                } else {
                    if (this.debug)
                        LOGGER.info("[{}] I am lower -> do not respond with OK", nodeId);
                }
                break;

            case "OK":
                if (this.debug)
                    LOGGER.info("[{}] Received OK from {}", nodeId, sender);
                lastElectionTimestamp.set(System.currentTimeMillis());
                break;

            case "COORDINATOR":
                if (this.debug)
                    LOGGER.info("[{}] Received COORDINATOR from {}", nodeId, sender);
                coordinatorId = sender;
                coordinatorListener.accept(sender);
                break;

            default:
                if (this.debug)
                    LOGGER.info("[{}] Unknown msg: {}", nodeId, msg);
        }
    }

    public Future<Boolean> startElection() {
        if (this.debug)
            LOGGER.info("[{}] External trigger -> startElection()", nodeId);
        return executor.submit(this::startElectionInternal);
    }

    private boolean startElectionInternal() {
        try {

            long sendTs = System.currentTimeMillis();
            if (this.debug)
                LOGGER.info("[{}] Starting election... ts={}", nodeId, sendTs);
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
                    LOGGER.info("[{}] Waiting... waited={} lastOkTs={} startTs={}", nodeId, waited, last, startTs);
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
            LOGGER.error("[{}] Error -> ", e.getMessage());
            return false;
        }
    }

    public boolean isLeader() {
        return Objects.equals(this.nodeId, this.coordinatorId);
    }

}
