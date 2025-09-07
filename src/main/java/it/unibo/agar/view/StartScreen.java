package it.unibo.agar.view;

import it.unibo.agar.Main;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class StartScreen extends JFrame {

    private final JTextField nameField = new JTextField(20);
    private final JTextField hostField = new JTextField(20);
    private final JButton startButton = new JButton("Start Game");
    private final JLabel statusLabel = new JLabel(" ");

    public StartScreen() {
        super("AgarIO - Start game");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        initLayout();
        pack();
        setLocationRelativeTo(null);
    }

    private void initLayout() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(6, 6, 6, 6);
        gbc.anchor = GridBagConstraints.WEST;

        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(new JLabel("Player name:"), gbc);

        gbc.gridx = 1;
        panel.add(nameField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 1;
        panel.add(new JLabel("Broker IP address:"), gbc);

        gbc.gridx = 1;
        panel.add(hostField, gbc);

        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.gridwidth = 2;
        gbc.anchor = GridBagConstraints.CENTER;
        panel.add(startButton, gbc);

        gbc.gridy = 3;
        panel.add(statusLabel, gbc);

        startButton.addActionListener(new StartAction());

        getContentPane().add(panel, BorderLayout.CENTER);
    }

    private class StartAction implements ActionListener {
        @Override
        public void actionPerformed(ActionEvent e) {
            String playerName = nameField.getText().trim();
            String hostAddress = hostField.getText().trim();

            if (playerName.isEmpty()) {
                statusLabel.setText("Insert player name.");
                return;
            }
            if (hostAddress.isEmpty()) {
                statusLabel.setText("Insert broker address.");
                return;
            }

            startButton.setEnabled(false);
            statusLabel.setText("Starting...");

            SwingUtilities.invokeLater(() -> {
                setVisible(false);
                dispose();
                String[] args = new String[]{playerName, hostAddress};
                new Thread(() -> Main.startGame(args), "GameStarterThread").start();
            });
        }
    }

    public static void showAndWait() {
        SwingUtilities.invokeLater(() -> {
            StartScreen ss = new StartScreen();
            ss.setVisible(true);
        });
    }
}

