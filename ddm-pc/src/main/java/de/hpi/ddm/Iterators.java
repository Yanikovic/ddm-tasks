package de.hpi.ddm;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Iterators {

    public static void main(String[] args) {
        Iterator<String> combinations = new CombinationRepetition(new char[]{'A', 'B', 'C', 'D', 'E'}, 6).iterator();
        int ctr = 0;
        while(combinations.hasNext()) {
            System.out.println(combinations.next());
            ctr++;
        }
        System.out.println(ctr);
        /*
        for (String sequence: new CombinationRepetition(new char[]{'A', 'B', 'C', 'D'}, 3)) {
            System.out.println(sequence);
        }
        */
    }

    public static class CombinationRepetition implements Iterable<String> {

        private final char[] universe;
        private final int passwordLength;

        public CombinationRepetition(char[] universe, int passwordLength) {
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

    public static class Permutation implements Iterable<String> {

        private final char[] universe;
        private final int length;

        public Permutation(char[] universe) {
            this.universe = universe;
            this.length = universe.length;
        }

        @Override
        public Iterator<String> iterator() {
            return new Iterator<String>() {

                private final char[] permutationArray = Arrays.copyOf(universe, universe.length);
                private int[] index = (length == 0) ? null : new int[length];

                @Override
                public boolean hasNext() {
                    return index != null;
                }

                @Override
                public String next() {
                    if (index == null) throw new NoSuchElementException();

                    for (int i = 1; i < length; ++i) {
                        char swap = permutationArray[i];
                        System.arraycopy(permutationArray, 0, permutationArray, 1, i);
                        permutationArray[0] = swap;
                        for (int j = 1; j < i; ++j) {
                            index[j] = 0;
                        }
                        if (++index[i] <= i) {
                            return new String(permutationArray);
                        }
                        index[i] = 0;
                    }
                    index = null;
                    return new String(permutationArray);
                }
            };
        }
    }


    @AllArgsConstructor
    public static class CombinationNoRepetition implements Iterable<String> {

        private final char[] universe;
        private final int seqLength;


        @Override
        public Iterator<String> iterator() {
            return new CombinationIterator();
        }

        @Data
        private class CombinationIterator implements Iterator<String> {

            private int[] pointers;
            private int r;
            private int i;

            public CombinationIterator() {
                this.pointers = new int[seqLength];
                this.r = 0;
                this.i = 0;
            }

            @Override
            public boolean hasNext() {
                return r >= 0;
            }

            @Override
            public String next() {
                return generateCombination();
            }

            private String generateCombination() {
                String sequence = "";
                while (r >= 0) {
                    // forward step if i < (N + (r-K))
                    if (i <= (universe.length + (r - seqLength))) {
                        pointers[r] = i;
                        // if combination array is full print and increment i;
                        if (r == seqLength - 1) {
                            sequence = generateSequence();
                            i++;
                            return sequence;
                        } else {
                            // if combination is not full yet, select next element
                            i = pointers[r] + 1;
                            r++;
                        }
                    }
                    // backward step
                    else {
                        r--;
                        if (r >= 0)
                            i = pointers[r] + 1;
                    }
                }
                return sequence;
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
}
