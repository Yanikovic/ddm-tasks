package de.hpi.ddm.utils;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Iterator;

@AllArgsConstructor
public class CombinationsNoRepetition implements Iterable<String> {

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
