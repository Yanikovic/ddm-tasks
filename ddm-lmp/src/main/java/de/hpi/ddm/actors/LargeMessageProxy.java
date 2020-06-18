package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializers;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class LargeMessageProxy extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "largeMessageProxy";
    public static final int DEFAULT_BATCH_SIZE = 128000;


    public static Props props() {
        return Props.create(LargeMessageProxy.class);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PostLargeMessage implements Serializable {
        private static final long serialVersionUID = -3132821777758085789L;
        private ActorRef sender;
        private ActorRef receiver;
        private int serializerID;
        private String manifest;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PreLargeMessage implements Serializable {
        private static final long serialVersionUID = 6970977148928832981L;
        private int expectedNumberOfBytes;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LargeMessage<T> implements Serializable {
        private static final long serialVersionUID = 2940665245810221108L;
        private T message;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BytesBatchMessage implements Serializable {
        private static final long serialVersionUID = 4057807743872319842L;
        private byte[] bytes;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LocalMessage<T> implements Serializable {
        private static final long serialVersionUID = -7642088498409543951L;
        private T bytes;
        private ActorRef sender;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AssembledLargeMessage implements Serializable {
        private static final long serialVersionUID = 9009967346569634538L;
        private Object message;
        private ActorRef sender;
        private ActorRef receiver;
    }

    /////////////////
    // Actor State //
    /////////////////

    private ByteBuffer receiverProxyAssembler;
    private final Serialization serialization = SerializationExtension.get(this.context().system());

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LargeMessage.class, this::handle)
                .match(LocalMessage.class, this::handle)
                .match(BytesBatchMessage.class, this::handle)
                .match(PreLargeMessage.class, this::handle)
                .match(PostLargeMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(PreLargeMessage preLargeMessage) {
        receiverProxyAssembler = ByteBuffer.allocate(preLargeMessage.getExpectedNumberOfBytes());
    }

    private void handle(PostLargeMessage postLargeMessage) {
        byte[] serializedLargeMessage = receiverProxyAssembler.array();
        receiverProxyAssembler = null;
        Object deserializedLargeMessage = deserialize(serializedLargeMessage, postLargeMessage);

        postLargeMessage.getReceiver().tell(
                new AssembledLargeMessage(deserializedLargeMessage, postLargeMessage.getSender(), postLargeMessage.getReceiver()),
                postLargeMessage.getSender());
    }

    private void handle(LargeMessage<?> message) {
        ActorRef receiver = message.getReceiver();
        ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
        if (receiverProxy.anchorPath().toString().contains("@")) {
            System.out.println("remote");
            handleRemote(receiverProxy, message);
        } else {
            System.out.println("local");
            receiverProxy.tell(new LocalMessage<>(message.getMessage(), this.sender(), message.getReceiver()),
                    this.self());
        }
    }

    private void handleRemote(ActorSelection receiverProxy, LargeMessage<?> message) {
        byte[] bytes = serialize(message.getMessage(), serialization);
        int serializerID = serialization.findSerializerFor(message.getMessage()).identifier();
        String manifest = Serializers.manifestFor(serialization.findSerializerFor(message.getMessage()), message.getMessage());

        receiverProxy.tell(new PreLargeMessage(bytes.length), this.self());

        byte[][] byteBatches = splitSerializedObject(bytes);
        System.out.println(byteBatches.length);

        /*
        system.scheduler().scheduleOnce(
        Duration.ofMillis(50), testActor, "foo", system.dispatcher(), ActorRef.noSender());
         */

        for (byte[] batch : byteBatches) {
            receiverProxy.tell(new BytesBatchMessage(batch), this.self());
        }
        // construct message for receiver proxy
        // in the message we encode the master as the sender and the worker as the receiver (no proxy info included)
        // this.self is the actor ref to ourselves (in this case the sender side of the proxy)
        // this.sender is the sender of the large message we currently deal with (master)
        // message.getReceiver is the true receiver of the large message which is a worker
        receiverProxy
                .tell(new PostLargeMessage(this.sender(), message.getReceiver(), serializerID, manifest),
                        this.self());
    }

    private void handle(BytesBatchMessage message) {
        // Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
        receiverProxyAssembler.put(message.getBytes());
    }

    private void handle(LocalMessage<?> message) {
        message.getReceiver().tell(message.getBytes(), message.getSender());
    }

    ////////////////////
    // Actor Utils    //
    ////////////////////

    private <T> byte[] serialize(T message, Serialization serialization) {
        return serialization.serialize(message).get();
    }

    private Object deserialize(byte[] bytes, PostLargeMessage serializationInfo) {
        int serializerID = serializationInfo.getSerializerID();
        String manifest = serializationInfo.getManifest();
        return serialization.deserialize(bytes, serializerID, manifest).get();
    }

    private static byte[][] splitSerializedObject(byte[] bytes) {
        final int length = bytes.length;
        if (DEFAULT_BATCH_SIZE > length) {
            return new byte[][]{bytes};
        }
        final int numberOfBatches = (length + DEFAULT_BATCH_SIZE - 1) / DEFAULT_BATCH_SIZE;
        byte[][] batches = new byte[numberOfBatches][];
        int batchIndex = 0;

        for (int batchStartIndex = 0; batchStartIndex < length - DEFAULT_BATCH_SIZE + 1; batchStartIndex += DEFAULT_BATCH_SIZE) {
            batches[batchIndex++] = Arrays.copyOfRange(bytes, batchStartIndex, batchStartIndex + DEFAULT_BATCH_SIZE);
        }
        if (length % DEFAULT_BATCH_SIZE != 0) {
            batches[batchIndex] = Arrays.copyOfRange(bytes, length - (length % DEFAULT_BATCH_SIZE), length);
        }
        return batches;
    }
}
