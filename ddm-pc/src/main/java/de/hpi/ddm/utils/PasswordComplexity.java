package de.hpi.ddm.utils;

import de.hpi.ddm.structures.PasswordInfo;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PasswordComplexity {

    private int numChars;
    private int numHints;
    private int passwordLength;
    private int hintLength;
    private int numCharsUsedForPassword;
    private int numHintsToCrack;

    public PasswordComplexity(PasswordInfo passwordInfo) {
        this.numChars = passwordInfo.getUniverse().length;
        this.numHints = passwordInfo.getHintHashes().length;
        this.passwordLength = passwordInfo.getPasswordLength();
        this.hintLength = numChars - 1;
        this.numCharsUsedForPassword = numChars - numHints;
    }

    public void calcNumberOfHintsToCrack() {
        int hintComplexity = getHintComplexity();
        int[] pwComplexityWithHints = getPasswordComplexityWithHints();
        for (int i = 0; i < pwComplexityWithHints.length; i++) {
            if (pwComplexityWithHints[i] < hintComplexity) {
                this.numHintsToCrack = i;
                return;
            }
        }
        this.numHintsToCrack = numHints;
    }

    private int getHintComplexity() {
        return factorial(numChars);
    }

    private int[] getPasswordComplexityWithHints() {
        int[] complexityAfterEachHint = new int[numHints + 1];
        for (int hintsChecked = 0; hintsChecked < complexityAfterEachHint.length; hintsChecked++) {
            complexityAfterEachHint[hintsChecked] = getPasswordComplexity(numChars - hintsChecked);
        }
        return complexityAfterEachHint;
    }

    private int getPasswordComplexity(int numOfKnownChars) {
        int guessPasswordChars = binomialCoefficient(numOfKnownChars, numCharsUsedForPassword);
        int guessPasswordWithChars = (int) Math.pow(numCharsUsedForPassword, passwordLength);
        return guessPasswordChars * guessPasswordWithChars;
    }

    private int binomialCoefficient(int n, int k) {
        return factorial(n) / (factorial(n - k) * factorial(k));
    }

    private int factorial(int x) {
        if (x >= 1) {
            return x * factorial(x - 1);
        } else {
            return 1;
        }
    }
}
