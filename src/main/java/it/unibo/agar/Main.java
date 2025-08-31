package it.unibo.agar;

import it.unibo.agar.model.*;
import it.unibo.agar.view.GlobalView;
import it.unibo.agar.view.LocalView;

import javax.swing.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class Main {

    private static final int WORLD_WIDTH = 1000;
    private static final int WORLD_HEIGHT = 1000;
    private static final int NUM_PLAYERS = 4; // p1, p2, p3, p4
    private static final int NUM_FOODS = 100;
    private static final long GAME_TICK_MS = 20; // Corresponds to ~33 FPS

    public static void main(String[] args) {

        if (args.length != 2) {
            System.err.println("Usage: java Main <playerName> <hostAddress>");
            return;
        }

        String playerName = args[0];
        String hostAddress = args[1];

        final GameStateManager gameManager;
        try {
            gameManager = new DistributedGameStateManager(hostAddress, playerName);
        } catch (IOException | TimeoutException e) {
            System.err.println("Error during connection: " + e.getMessage());
            return;
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        // List to keep track of active views for repainting
        final List<JFrameRepaintable> views = new ArrayList<>();

        SwingUtilities.invokeLater(() -> {
            GlobalView globalView = new GlobalView(gameManager);
            views.add(globalView::repaintView); // Add repaint method reference
            globalView.setVisible(true);

            LocalView localViewP1 = new LocalView(gameManager, playerName);
            views.add(localViewP1::repaintView);
            localViewP1.setVisible(true);
        });
        final Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                AIMovement.moveAI(playerName, gameManager);

                try {
                    gameManager.tick();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                SwingUtilities.invokeLater(() -> {
                    for (JFrameRepaintable view : views) {
                        view.repaintView();
                    }
                });
            }
        }, 0, GAME_TICK_MS);
    }

    // Functional interface for repaintable views
    @FunctionalInterface
    interface JFrameRepaintable {
        void repaintView();
    }
}
