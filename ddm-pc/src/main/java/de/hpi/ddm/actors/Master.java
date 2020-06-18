package de.hpi.ddm.actors;

import akka.actor.*;
import de.hpi.ddm.structures.PasswordInfo;
import de.hpi.ddm.utils.PasswordComplexity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.*;

/*
TODO:
- hint workloads erstellen
- abarbeitung von hints implementieren
- update der passwords im master anhand der geknackten hints implementieren
- routine zum Erstellen eines password workloads, wenn alle hints geknackt sind
 */
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
    public static class InitialInfoMessage implements Serializable {
        private static final long serialVersionUID = -4780440373749233127L;
        private String[] line;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<String[]> lines;
    }

    @Data
    public static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
    }

    @Data
    public static class PullRequest implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
    }

    @Data
    @AllArgsConstructor
    public static class HintSuccessMessage implements Serializable {
        private static final long serialVersionUID = -8116992589322209006L;
        private int passwordID;
        private String hint;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef reader;
    private final ActorRef collector;
    private final List<ActorRef> workers;

    private PasswordComplexity passwordComplexity;
    private Queue<Worker.Workload> workloads;
    private HashMap<Integer, PasswordInfo> passwords;

    private long startTime;

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);
        workloads = new LinkedList<>();
        passwords = new HashMap<>(1000);
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::handle)
                .match(InitialInfoMessage.class, this::handle)
                .match(BatchMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .match(RegistrationMessage.class, this::handle)
                .match(PullRequest.class, this::handle)
                .match(HintSuccessMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(HintSuccessMessage hintSuccessMessage) {
        int id = hintSuccessMessage.getPasswordID();
        String hint = hintSuccessMessage.getHint();
        PasswordInfo pwInfo = passwords.get(id);
        System.out.println(pwInfo);
        pwInfo.applyHint(hint);
        pwInfo.incrementHintIndex();
        passwords.put(id, pwInfo);
        System.out.println("Info: " + pwInfo);

        if (pwInfo.getCurrHintIndex() < passwordComplexity.getNumHintsToCrack()) {
            // Hint workload
            createHintWorkload(id, pwInfo);
        } else {
            // password workload
            createPasswordWorkload(id, pwInfo);
        }
    }

    private void handle(PullRequest pullRequest) {
        if (!workloads.isEmpty()) {
            this.sender().tell(workloads.remove(), this.self());
        } else {
            this.sender().tell(new Worker.EmptyWorkload(), this.self());
        }
    }

    protected void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();
        this.reader.tell(new Reader.InitialInfoRequest(), this.self());
    }

    protected void handle(InitialInfoMessage message) {
        PasswordInfo initialInfo = new PasswordInfo(message.line);
        passwordComplexity = new PasswordComplexity(initialInfo);
        passwordComplexity.calcNumberOfHintsToCrack();

        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    protected void handle(BatchMessage message) {
        if (message.getLines().isEmpty()) {
            this.collector.tell(new Collector.PrintMessage(), this.self());
            //this.terminate();
            return;
        }
        processBatch(message.getLines());
        //System.out.println(workloads.size());
        this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    private void processBatch(List<String[]> lines) {
        for (String[] line : lines) {
            int pwID = Integer.parseInt(line[0]);
            PasswordInfo pwInfo = new PasswordInfo(line);
            passwords.put(pwID, pwInfo);


            passwordComplexity.setNumHintsToCrack(2);

            if (passwordComplexity.getNumHintsToCrack() == 0) {
                createPasswordWorkload(pwID, pwInfo);
            } else {
                createHintWorkload(pwID, pwInfo);
            }
        }
    }

    private void createHintWorkload(int passwordID, PasswordInfo passwordInfo) {
        workloads.offer(new Worker.HintWorkload(passwordID,
                passwordInfo.getUniverse(),
                passwordInfo.getCurrentHint()));
    }

    private void createPasswordWorkload(int passwordID, PasswordInfo passwordInfo) {
        workloads.offer(new Worker.PasswordWorkload(passwordID,
                passwordInfo.getPasswordChars(),
                passwordInfo));
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

    protected void handle(RegistrationMessage message) {
        this.context().watch(this.sender());
        this.workers.add(this.sender());
//		this.log().info("Registered {}", this.sender());
    }

    protected void handle(Terminated message) {
        this.context().unwatch(message.getActor());
        this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
    }
}
