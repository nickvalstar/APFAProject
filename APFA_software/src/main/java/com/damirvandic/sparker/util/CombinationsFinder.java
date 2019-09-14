package com.damirvandic.sparker.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CombinationsFinder {
    public static <T> Set<Set<T>> getCombinationsFor(List<T> group, int subsetSize) {
        Set<Set<T>> resultingCombinations = new HashSet<>();
        int totalSize = group.size();
        if (subsetSize == 0) {
            emptySet(resultingCombinations);
        } else if (subsetSize <= totalSize) {
            List<T> remainingElements = new ArrayList<>(group);
            T X = popLast(remainingElements);

            Set<Set<T>> combinationsExclusiveX = getCombinationsFor(remainingElements, subsetSize);
            Set<Set<T>> combinationsInclusiveX = getCombinationsFor(remainingElements, subsetSize - 1);
            for (Set<T> combination : combinationsInclusiveX) {
                combination.add(X);
            }
            resultingCombinations.addAll(combinationsExclusiveX);
            resultingCombinations.addAll(combinationsInclusiveX);
        }
        return resultingCombinations;
    }

    private static <T> void emptySet(Set<Set<T>> resultingCombinations) {
        resultingCombinations.add(new HashSet<T>());
    }

    private static <T> T popLast(List<T> elementsExclusiveX) {
        return elementsExclusiveX.remove(elementsExclusiveX.size() - 1);
    }
}
