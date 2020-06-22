package de.hpi.ddm.structures;

import it.unimi.dsi.fastutil.chars.CharArraySet;
import it.unimi.dsi.fastutil.chars.CharSet;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Arrays;

@Data
@NoArgsConstructor
public class PasswordInfo implements Serializable {

    private static final long serialVersionUID = 4649112340110301180L;
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

    public int getNumberOfUniqueCharsUsed() {
        return universe.length - hintHashes.length;
    }

    public void incrementHintIndex() {
        currHintIndex++;
    }

    public void applyHint(String hint) {
        CharSet hintAsSet = new CharArraySet(hint.toCharArray());
        CharSet currPasswordCharsAsSet = new CharArraySet(Arrays.copyOf(passwordChars, passwordChars.length));
        currPasswordCharsAsSet.removeAll(hintAsSet);

        CharSet witchHint = new CharArraySet(Arrays.copyOf(passwordChars, passwordChars.length));
        witchHint.removeAll(currPasswordCharsAsSet);

        passwordChars = witchHint.toCharArray();
    }
}
