package com.hazelcast.simulator.visualiser;

import com.hazelcast.simulator.visualiser.ui.MainUI;

import javax.swing.*;

public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                new MainUI();
            }
        });
    }
}
