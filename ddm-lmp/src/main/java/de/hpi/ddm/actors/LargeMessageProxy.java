package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
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
    public static final int DEFAULT_BATCH_SIZE = 100000;

    public static Props props() {
        return Props.create(LargeMessageProxy.class);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

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
    public static class BytesMessage<T> implements Serializable {
        private static final long serialVersionUID = 4057807743872319842L;
        private T bytes;
        private ActorRef sender;
        private ActorRef receiver;
    }

    /////////////////
    // Actor State //
    /////////////////

    private ByteBuffer target;

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
                .match(BytesMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(LargeMessage<?> message) {
        ActorRef receiver = message.getReceiver();
        // select receiver proxy on the receiving actor system via remote
        // select via receiver path
        // receiver is really a worker actor
        ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

        Serialization serialization = SerializationExtension.get(this.context().system());
        byte[] bytes = serialization.serialize(message).get();
        byte[][] byteBatches = splitSerializedObject(bytes);

        // This will definitely fail in a distributed setting if the serialized message is large!
        // Solution options:
        // 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
        // 2. Serialize the object and send its bytes via Akka streaming.
        // 3. Send the object via Akka's http client-server component.
        // 4. Other ideas ...

        // construct message for receiver proxy, translate large message into bytes message
        // in the message we encode the master as the sender and the worker as the receiver (no proxy info included)
        for (byte[] batch : byteBatches) {
            receiverProxy.tell(new BytesMessage<>(batch, this.sender(), message.getReceiver()), this.self());
        }
    }

    private void handle(BytesMessage<?> message) {
        // Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
        System.out.println(message.getReceiver());
        //message.getReceiver().tell(message.getBytes(), message.getSender());
    }

    ////////////////////
    // Actor Utils    //
    ////////////////////

    private static byte[] mergeSerializedObject(byte[][] byteBatches, int length) {
        ByteBuffer target = ByteBuffer.wrap(new byte[length]);
        for (byte[] batch : byteBatches) {
            target.put(batch);
        }
        return target.array();
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
