package de.hpi.ddm.actors;

import akka.actor.*;
import de.hpi.ddm.structures.PasswordInfo;
import de.hpi.ddm.utils.PasswordComplexity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.*;

public class Master extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";

    public static Props props(final ActorRef reader, final ActorRef collector) {
        return Props.create(Master.class, () -> new Master(reader, collector));
    }

    public Master(final ActorRef reader, final ActorRef collector) {
        this.reader = reader;
        this.collector = collector;
        this.workers = new ArrayList<>();
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    public static class StartMessage implements Serializable {
        private static final long serialVersionUID = -50374816448627600L;
    }

    @Data
    @AllArgsConstructor
    public static class InitialInfoResponse implements Serializable {
        private static final long serialVersionUID = -4780440373749233127L;
        private String[] line;
    }

    @Data
    public static class FinishedReadingResponse implements Serializable {
        private static final long serialVersionUID = 6569477591743716363L;
    }

    @Data
    public static class ShutdownMessage implements Serializable {
        private static final long serialVersionUID = 1147276424227641153L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<String[]> lines;
    }

    @Data
    @NoArgsConstructor
    public static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class HintSuccessMessage implements Serializable {
        private static final long serialVersionUID = -8116992589322209006L;
        private int passwordID;
        private String hint;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class PasswordSuccessMessage implements Serializable {
        private static final long serialVersionUID = 7452079235007937570L;
        private int passwordID;
        private String password;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef reader;
    private final ActorRef collector;
    private final List<ActorRef> workers;

    private PasswordComplexity passwordComplexity;
    private Map<Integer, PasswordInfo> passwords;

    private Queue<Worker.Workload> unassignedWork;
    private Queue<ActorRef> idleWorkers;
    private Map<ActorRef, Worker.Workload> busyWorkers;

    private long startTime;

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);
        unassignedWork = new LinkedList<>();
        idleWorkers = new LinkedList<>();
        busyWorkers = new HashMap<>();
        passwords = new HashMap<>(200);
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::handle)
                .match(InitialInfoResponse.class, this::handle)
                .match(BatchMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .match(FinishedReadingResponse.class, this::handle)
                .match(ShutdownMessage.class, this::handle)
                .match(RegistrationMessage.class, this::handle)
                .match(HintSuccessMessage.class, this::handle)
                .match(PasswordSuccessMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    protected void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();
        this.reader.tell(new Reader.InitialInfoRequest(), this.self());
    }

    protected void handle(RegistrationMessage message) {
        this.context().watch(this.sender());
        this.workers.add(this.sender());
        this.idleWorkers.add(this.sender());
        assignWork(this.sender());
        this.log().info("Registered {}", this.sender());
    }

    protected void handle(InitialInfoResponse message) {
        PasswordInfo initialInfo = new PasswordInfo(message.line);
        passwordComplexity = new PasswordComplexity(initialInfo);
        passwordComplexity.calcNumberOfHintsToCrack();

        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    protected void handle(PasswordSuccessMessage passwordSuccessMessage) {
        int id = passwordSuccessMessage.getPasswordID();
        String password = passwordSuccessMessage.getPassword();
        PasswordInfo passwordInfo = passwords.remove(id);
        String result = String.format("Result(ID: %d, Name: %s, Password: %s)", id, passwordInfo.getName(), password);
        this.collector.tell(new Collector.CollectMessage(result), this.self());

        reassignWork(this.sender());

        if (passwords.isEmpty() && unassignedWork.isEmpty() && busyWorkers.isEmpty()) {
            this.reader.tell(new Reader.FinishedReadingRequest(), this.self());
        }
    }

    protected void handle(HintSuccessMessage hintSuccessMessage) {
        int id = hintSuccessMessage.getPasswordID();
        String hint = hintSuccessMessage.getHint();
        PasswordInfo pwInfo = passwords.get(id);
        pwInfo.applyHint(hint);
        pwInfo.incrementHintIndex();
        passwords.put(id, pwInfo);

        if (pwInfo.getCurrHintIndex() == passwordComplexity.getNumHintsToCrack()) {
            createPasswordWorkload(id, pwInfo);
        }
        reassignWork(this.sender());
    }

    private void reassignWork(ActorRef worker) {
        if (busyWorkers.containsKey(worker)) {
            busyWorkers.remove(worker);
            idleWorkers.add(worker);
            assignWork(worker);
        }
    }

    protected void handle(BatchMessage message) {
        if (message.getLines().isEmpty()) {
            return;
        }
        processBatch(message.getLines());
        assignWork();
        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    private void processBatch(List<String[]> lines) {
        for (String[] line : lines) {
            int pwID = Integer.parseInt(line[0]);
            PasswordInfo pwInfo = new PasswordInfo(line);
            passwords.put(pwID, pwInfo);

            if (passwordComplexity.getNumHintsToCrack() == 0) {
                createPasswordWorkload(pwID, pwInfo);
            } else {
                createHintWorkloads(pwID, pwInfo);
            }
        }
    }

    protected void handle(Terminated message) {
        ActorRef terminatedWorker = message.getActor();
        this.context().unwatch(terminatedWorker);
        this.workers.remove(terminatedWorker);
        if (busyWorkers.containsKey(terminatedWorker)) {
            Worker.Workload work = busyWorkers.remove(terminatedWorker);
            assignWork(work);
        } else {
            idleWorkers.remove(terminatedWorker);
        }
        this.log().info("Unregistered {}", message.getActor());
    }

    private void createHintWorkloads(int passwordID, PasswordInfo passwordInfo) {
        String[] hintHashes = passwordInfo.getHintHashes();
        for (int i = 0; i < passwordComplexity.getNumHintsToCrack(); i++) {
            unassignedWork.add(new Worker.HintWorkload(passwordID,
                    passwordInfo.getUniverse(),
                    hintHashes[i]));
        }
    }

    private void createPasswordWorkload(int passwordID, PasswordInfo passwordInfo) {
        unassignedWork.add(new Worker.PasswordWorkload(passwordID,
                passwordInfo.getPasswordChars(),
                passwordInfo));
    }

    private void assignWork(ActorRef worker) {
        if (!unassignedWork.isEmpty()) {
            Worker.Workload work = unassignedWork.remove();
            worker.tell(work, this.self());
            idleWorkers.remove(worker);
            busyWorkers.put(worker, work);
        }
    }

    private void assignWork(Worker.Workload work) {
        if (!idleWorkers.isEmpty()) {
            ActorRef idleWorker = idleWorkers.remove();
            idleWorker.tell(work, this.self());
            busyWorkers.put(idleWorker, work);
        } else {
            unassignedWork.add(work);
        }
    }

    private void assignWork() {
        if (idleWorkers.isEmpty() || unassignedWork.isEmpty()) {
            return;
        }
        ActorRef worker = idleWorkers.remove();
        Worker.Workload work = unassignedWork.remove();
        worker.tell(work, this.self());
        busyWorkers.put(worker, work);
        assignWork();
    }

    private void handle(FinishedReadingResponse finishedMessage) {
        this.collector.tell(new Collector.PrintMessage(), this.self());
    }

    private void handle(ShutdownMessage shutdownMessage) {
        this.terminate();
    }

    protected void terminate() {
        this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
        this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());

        for (ActorRef worker : this.workers) {
            this.context().unwatch(worker);
            worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }

        this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());

        long executionTime = System.currentTimeMillis() - this.startTime;
        this.log().info("Algorithm finished in {} ms", executionTime);
    }
}
