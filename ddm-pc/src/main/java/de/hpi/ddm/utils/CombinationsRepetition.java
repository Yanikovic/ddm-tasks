package de.hpi.ddm.utils;

import java.util.Arrays;
import java.util.Iterator;

public class CombinationsRepetition implements Iterable<String> {

    private final char[] universe;
    private final int passwordLength;

    public CombinationsRepetition(char[] universe, int passwordLength) {
        this.universe = universe;
        this.passwordLength = passwordLength;
    }

    @Override
    public Iterator<String> iterator() {
        return new CombinationIterator();
    }

    private class CombinationIterator implements Iterator<String> {
        private int[] pointers;
        private int[] lastCombination;
        private boolean finished;

        public CombinationIterator() {
            initArrays();
        }

        private void initArrays() {
            pointers = new int[passwordLength];
            lastCombination = new int[passwordLength];
            for (int i = 0; i < passwordLength; i++) {
                pointers[i] = 0;
                lastCombination[i] = universe.length - 1;
            }
        }

        @Override
        public boolean hasNext() {
            return !finished;
        }

        @Override
        public String next() {
            String sequence = generateSequence();
            if (!Arrays.equals(pointers, lastCombination)) {
                updatePointers();
            } else {
                finished = true;
            }
            return sequence;
        }

        private void updatePointers() {
            for (int i = pointers.length - 1; i >= 0; i--) {
                if (pointers[i] + 1 <= lastCombination[i]) {
                    pointers[i] = pointers[i] + 1;
                    break;
                } else {
                    pointers[i] = 0;
                }
            }
        }

        private String generateSequence() {
            StringBuilder sb = new StringBuilder();
            for (int pointer : pointers) {
                sb.append(universe[pointer]);
            }
            return sb.toString();
        }
    }
}
