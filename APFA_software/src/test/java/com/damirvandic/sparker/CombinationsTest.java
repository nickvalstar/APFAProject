package com.damirvandic.sparker;

import com.damirvandic.sparker.util.CombinationsFinder;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class CombinationsTest {
    @Test
    public void testCombinations() {
        List<String> group = Arrays.asList(new String[]{"A", "B", "C", "D", "E"});
        Set<Set<String>> combs = CombinationsFinder.getCombinationsFor(group, 3);
        assertTrue(combs.contains(new HashSet<>(Arrays.asList(new String[]{"A", "B", "C"}))));
        assertTrue(combs.contains(new HashSet<>(Arrays.asList(new String[]{"A", "B", "D"}))));
        assertTrue(combs.contains(new HashSet<>(Arrays.asList(new String[]{"A", "C", "D"}))));
        assertTrue(combs.contains(new HashSet<>(Arrays.asList(new String[]{"A", "B", "E"}))));
        assertTrue(combs.contains(new HashSet<>(Arrays.asList(new String[]{"B", "C", "D"}))));
        assertTrue(combs.contains(new HashSet<>(Arrays.asList(new String[]{"A", "C", "E"}))));
        assertTrue(combs.contains(new HashSet<>(Arrays.asList(new String[]{"B", "C", "E"}))));
        assertTrue(combs.contains(new HashSet<>(Arrays.asList(new String[]{"A", "D", "E"}))));
        assertTrue(combs.contains(new HashSet<>(Arrays.asList(new String[]{"B", "D", "E"}))));
        assertTrue(combs.contains(new HashSet<>(Arrays.asList(new String[]{"C", "D", "E"}))));
    }
}
