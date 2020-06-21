package de.hpi.ddm.utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Permutations implements Iterable<String> {

    private final char[] universe;
    private final int length;

    public Permutations(char[] universe) {
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
