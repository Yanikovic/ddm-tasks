package de.hpi.ddm.structures;

import it.unimi.dsi.fastutil.chars.CharArraySet;
import it.unimi.dsi.fastutil.chars.CharSet;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Arrays;

@Data
@AllArgsConstructor
public class PasswordInfo {

    private String name;
    private char[] universe;
    private char[] passwordChars;
    private int passwordLength;
    private String passwordHash;
    private String[] hintHashes;
    private int currHintIndex;
    private String password;

    public PasswordInfo(String[] line) {
        this.name = line[1];
        this.universe = line[2].toCharArray();
        this.passwordChars = Arrays.copyOf(universe, universe.length);
        this.passwordLength = Integer.parseInt(line[3]);
        this.passwordHash = line[4];
        this.hintHashes = Arrays.copyOfRange(line, 5, line.length);
        this.currHintIndex = 0;
    }

    public String getCurrentHint() {
        return hintHashes[currHintIndex];
    }

    public int getNumberOfUniqueCharsUsed() {
        return universe.length - hintHashes.length;
    }

    public void incrementHintIndex() {
        currHintIndex++;
    }

    public void applyHint(String hint) {
        CharSet hintAsSet = new CharArraySet(hint.toCharArray());
        CharSet hintRemovedSet = new CharArraySet(Arrays.copyOf(passwordChars, passwordChars.length));
        hintRemovedSet.removeAll(hintAsSet);

        CharSet currCharSet = new CharArraySet(Arrays.copyOf(passwordChars, passwordChars.length));
        currCharSet.removeAll(hintRemovedSet);

        passwordChars = currCharSet.toCharArray();
    }
}
