package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.util.Timeout;
import de.hpi.ddm.Iterators;
import de.hpi.ddm.MasterSystem;
import de.hpi.ddm.structures.PasswordInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

public class Worker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "worker";

    public static Props props() {
        return Props.create(Worker.class);
    }

    public Worker() {
        this.cluster = Cluster.get(this.context().system());
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Workload implements Serializable {
        private static final long serialVersionUID = -9124610486395741813L;
        private int passwordID;
        private char[] universe;
        private String hash;
    }

    @Data
    public static class PasswordWorkload extends Workload implements Serializable {
        private static final long serialVersionUID = -8426611880712186309L;
        private int passwordLength;
        private int numCharsUsedForPassword;

        public PasswordWorkload(int passwordID, char[] universe, PasswordInfo info) {
            super(passwordID, universe, info.getPasswordHash());
            this.passwordLength = info.getPasswordLength();
            this.numCharsUsedForPassword = info.getNumberOfUniqueCharsUsed();
        }
    }

    @Data
    public static class HintWorkload extends Workload implements Serializable {
        private static final long serialVersionUID = 5183210312715474159L;

        public HintWorkload(int passwordID, char[] universe, String hash) {
            super(passwordID, universe, hash);
        }
    }

    @Data
    public static class EmptyWorkload extends Workload implements Serializable {
        private static final long serialVersionUID = 2632231490626996777L;
    }

    /////////////////
    // Actor State //
    /////////////////

    private Member masterSystem;
    private final Cluster cluster;

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);
        this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
    }

    @Override
    public void postStop() {
        this.cluster.unsubscribe(this.self());
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CurrentClusterState.class, this::handle)
                .match(MemberUp.class, this::handle)
                .match(MemberRemoved.class, this::handle)
                .match(PasswordWorkload.class, this::handle)
                .match(HintWorkload.class, this::handle)
                .match(EmptyWorkload.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(HintWorkload hintWorkload) {
        for (String permutation: new Iterators.Permutation(hintWorkload.getUniverse())) {
            String sequence = permutation.substring(0, permutation.length() - 1);
            String hash = getHash(sequence);
            if (hash.equals(hintWorkload.getHash())) {
                this.sender().tell(new Master.HintSuccessMessage(hintWorkload.getPasswordID(), sequence), this.self());
                this.sender().tell(new Master.PullRequest(), this.self());
                return;
            }
        }
        this.sender().tell(new Master.PullRequest(), this.self());
    }

    private void handle(EmptyWorkload emptyWorkload) {
        Timeout.apply(5, TimeUnit.SECONDS);
        this.sender().tell(new Master.PullRequest(), this.self());
    }

    private void handle(PasswordWorkload passwordWorkload) {
        Iterators.CombinationNoRepetition charCombinationIterator = new Iterators.CombinationNoRepetition(
                passwordWorkload.getUniverse(),
                passwordWorkload.getNumCharsUsedForPassword());

        for (String charCombination: charCombinationIterator) {
            if (checkHashesForCharCombination(passwordWorkload, charCombination.toCharArray())) {
                return;
            }
        }
        this.sender().tell(new Master.PullRequest(), this.self());
    }

    private boolean checkHashesForCharCombination(PasswordWorkload workload, char[] charsToUse) {
        Iterators.CombinationRepetition pwCombinationIterator = new Iterators.CombinationRepetition(
                charsToUse, workload.getPasswordLength());
        for (String combination: pwCombinationIterator) {
            String hash = getHash(combination);
            if (hash.equals(workload.getHash())) {
                this.sender().tell(new Master.PasswordSuccessMessage(workload.getPasswordID(), combination), this.self());
                this.sender().tell(new Master.PullRequest(), this.self());
                return true;
            }
        }
        return false;
    }


    private void handle(CurrentClusterState message) {
        message.getMembers().forEach(member -> {
            if (member.status().equals(MemberStatus.up()))
                this.register(member);
        });
    }

    private void handle(MemberUp message) {
        this.register(message.member());
    }

    private void register(Member member) {
        if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
            this.masterSystem = member;

            this.getContext()
                    .actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
                    .tell(new Master.RegistrationMessage(), this.self());

            this.getContext()
                    .actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
                    .tell(new Master.PullRequest(), this.self());
        }
    }

    private void handle(MemberRemoved message) {
        if (this.masterSystem.equals(message.member()))
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    private String getHash(String line) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes(StandardCharsets.UTF_8));

            StringBuilder stringBuilder = new StringBuilder();
            for (byte hashedByte : hashedBytes) {
                stringBuilder.append(Integer.toString((hashedByte & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuilder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}