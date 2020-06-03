package de.hpi.ddm;

import akka.actor.ActorSystem;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializers;
import de.hpi.ddm.actors.LargeMessageProxy;
import de.hpi.ddm.actors.Worker;
import de.hpi.ddm.structures.BloomFilter;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class TestSerialization {

    // TODO:
    // - Wie Gesamtgröße des byte arrays übermitteln
    // - Wie Reihenfolge der byte arrays gewährleisten
    // - Was, wenn eine zweite large message erzeugt wird und sich die byte arrays vermischen?

    private static final BloomFilter testData = new BloomFilter(BloomFilter.DEFAULT_SIZE, true);

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("serTest");
        Serialization serialization = SerializationExtension.get(system);
        //String testString = "Thorsten wir lieben dich";
        //System.out.println("object: " + testString);
        LargeMessageProxy.LargeMessage<BloomFilter> testMessage =
                new LargeMessageProxy.LargeMessage<>(testData, system.actorOf(Worker.props()));

        byte[] bytes = serialization.serialize(testMessage).get();
        int length = bytes.length;
        System.out.println(length);

        int serializerID = serialization.findSerializerFor(testMessage).identifier();
        String manifest = Serializers.manifestFor(serialization.findSerializerFor(testMessage), testMessage);

        byte[][] byteBatches = splitSerializedObject(bytes, 128000);
        System.out.println("Anzahl batches: " + byteBatches.length);
        //System.out.println("Byte array: " + Arrays.toString(bytes));
        //System.out.println("Byte array batch-wise: " + Arrays.deepToString(byteBatches));

        byte[] mergedByteArray = mergeSerializedObject(byteBatches, length);
        //System.out.println("Merged byte array: " + Arrays.toString(mergedByteArray));


        LargeMessageProxy.LargeMessage<?> deserialized =
                (LargeMessageProxy.LargeMessage<?>) serialization.deserialize(mergedByteArray, serializerID, manifest).get();
        System.out.println("Deserialized object: " + deserialized.getMessage());
        System.exit(0);
    }

    private static byte[] mergeSerializedObject(byte[][] byteBatches, int length) {
        ByteBuffer target = ByteBuffer.wrap(new byte[length]);
        for (byte[] batch: byteBatches) {
            target.put(batch);
        }
        return target.array();
    }


    private static byte[][] splitSerializedObject(byte[] bytes, int batchSize) {
        final int length = bytes.length;
        if (batchSize > length) {
            return new byte[][]{bytes};
        }
        final int numberOfBatches = (length + batchSize-1) / batchSize;
        byte[][] batches = new byte[numberOfBatches][];
        int batchIndex = 0;

        for (int batchStartIndex = 0; batchStartIndex < length - batchSize+1; batchStartIndex += batchSize) {
            batches[batchIndex++] = Arrays.copyOfRange(bytes, batchStartIndex, batchStartIndex + batchSize);
        }
        if (length % batchSize != 0) {
            batches[batchIndex] = Arrays.copyOfRange(bytes, length - (length % batchSize), length);
        }
        return batches;
    }
}
