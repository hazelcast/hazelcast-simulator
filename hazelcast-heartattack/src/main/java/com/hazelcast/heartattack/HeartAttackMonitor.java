package com.hazelcast.heartattack;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import static com.hazelcast.heartattack.Utils.readObject;

public class HeartAttackMonitor implements Runnable {
    final static ILogger log = Logger.getLogger(HeartAttackMonitor.class);

    private Coach coach;

    public HeartAttackMonitor(Coach coach) {
        this.coach = coach;
    }

    private void addIfNotNull(List<HeartAttack> heartAttacks, HeartAttack h) {
        if (h != null)
            heartAttacks.add(h);
    }

    public void run() {
        for (; ; ) {
            try {
                detect();
            } catch (Exception e) {
                log.log(Level.SEVERE, "Failed to scan for heart attacks", e);
            }
            Utils.sleepSeconds(1);
        }
    }

    private void detect() {
        final TraineeVmManager traineeVmManager = coach.getTraineeVmManager();

        for (TraineeVm jvm : traineeVmManager.getTraineeJvms()) {

            List<HeartAttack> heartAttacks = new LinkedList<HeartAttack>();

            addIfNotNull(heartAttacks, detectOomeHeartAttackFile(jvm));

            addIfNotNull(heartAttacks, detectUnexpectedExit(jvm));

            addIfNotNull(heartAttacks, detectMembershipFailure(jvm));

            if (!heartAttacks.isEmpty()) {
                traineeVmManager.destroy(jvm);

                for (HeartAttack heartAttack : heartAttacks) {
                    coach.heartAttack(heartAttack);
                }
            }
        }

        File workoutHome = coach.getWorkoutHome();
        if (workoutHome != null) {
            File[] files = workoutHome.listFiles();
            if (files != null) {
                for (File file : files) {
                    final String name = file.getName();
                    if (name.endsWith(".exception")) {
                        Throwable cause = (Throwable) readObject(file);
                        file.delete();

                        String traineeId = name.substring(0, name.indexOf('.'));
                        log.log(Level.INFO,"traineeId: "+traineeId);
                        TraineeVm jvm = traineeVmManager.getTrainee(traineeId);
                        HeartAttack heartAttack = new HeartAttack(
                                "Exception thrown in trainee",
                                coach.getCoachHz().getCluster().getLocalMember().getInetSocketAddress(),
                                jvm==null?null:jvm.getMember().getInetSocketAddress(),
                                traineeId,
                                coach.getExerciseRecipe(),
                                cause);
                        coach.heartAttack(heartAttack);
                        traineeVmManager.destroy(jvm);
                    }
                }
            }
        }
    }

    private HeartAttack detectMembershipFailure(TraineeVm jvm) {
        //if the jvm is not assigned a hazelcast address yet.
        if (jvm.getMember() == null) {
            return null;
        }

        Member member = findMember(jvm);
        if (member == null) {
            jvm.getProcess().destroy();
            return new HeartAttack("Hazelcast membership failure (member missing)",
                    coach.getCoachHz().getCluster().getLocalMember().getInetSocketAddress(),
                    jvm.getMember().getInetSocketAddress(),
                    jvm.getId(),
                    coach.getExerciseRecipe());
        }

        return null;
    }

    private Member findMember(TraineeVm jvm) {
        final HazelcastInstance traineeClient = coach.getTraineeVmManager().getTraineeClient();
        if (traineeClient == null) return null;

        for (Member member : traineeClient.getCluster().getMembers()) {
            if (member.getInetSocketAddress().equals(jvm.getMember().getInetSocketAddress())) {
                return member;
            }
        }

        return null;
    }

    private HeartAttack detectOomeHeartAttackFile(TraineeVm jvm) {
        File workoutDir = coach.getWorkoutHome();
        if (workoutDir == null) {
            return null;
        }

        File file = new File(workoutDir, jvm.getId() + ".oome");
        if (!file.exists()) {
            return null;
        }

        HeartAttack heartAttack = new HeartAttack(
                "out of memory",
                coach.getCoachHz().getCluster().getLocalMember().getInetSocketAddress(),
                jvm.getMember().getInetSocketAddress(),
                jvm.getId(),
                coach.getExerciseRecipe());
        jvm.getProcess().destroy();
        return heartAttack;
    }

    private HeartAttack detectUnexpectedExit(TraineeVm jvm) {
        Process process = jvm.getProcess();
        try {
            if (process.exitValue() != 0) {
                return new HeartAttack(
                        "exit code not 0",
                        coach.getCoachHz().getCluster().getLocalMember().getInetSocketAddress(),
                        jvm.getMember().getInetSocketAddress(),
                        jvm.getId(),
                        coach.getExerciseRecipe());
            }
        } catch (IllegalThreadStateException ignore) {
        }
        return null;
    }
}
