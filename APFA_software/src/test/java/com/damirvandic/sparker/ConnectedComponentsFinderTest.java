package com.damirvandic.sparker;

import com.damirvandic.sparker.core.ConnectedComponentsFinder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class ConnectedComponentsFinderTest {

    @Test
    public void testBasic() {

        Set<Pair<String, String>> edges = new HashSet<>();
        edges.add(new ImmutablePair<>("A", "B"));
        edges.add(new ImmutablePair<>("B", "C"));
        edges.add(new ImmutablePair<>("D", "C"));
        edges.add(new ImmutablePair<>("B", "D"));
        edges.add(new ImmutablePair<>("B", "E"));
        edges.add(new ImmutablePair<>("E", "F"));
        edges.add(new ImmutablePair<>("G", "H"));
        edges.add(new ImmutablePair<>("G", "I"));
        edges.add(new ImmutablePair<>("H", "I"));
        edges.add(new ImmutablePair<>("H", "G"));
        edges.add(new ImmutablePair<>("I", "G"));
        edges.add(new ImmutablePair<>("I", "H"));
        edges.add(new ImmutablePair<>("J", "K"));
        edges.add(new ImmutablePair<>("L", "K"));
        edges.add(new ImmutablePair<>("J", "M"));
        edges.add(new ImmutablePair<>("N", "M"));

        ConnectedComponentsFinder clustering = new ConnectedComponentsFinder<>(edges);
        Set<Set<String>> clusters = clustering.findClusters();
        assertTrue(clusters.contains(ImmutableSet.of("A", "B", "C", "D", "E", "F")));
        assertTrue(clusters.contains(ImmutableSet.of("G", "H", "I")));
        assertTrue(clusters.contains(ImmutableSet.of("J", "K", "L", "M", "N")));
    }

    @Test
    public void multiMapTest() {
        Multimap<String, String> map = HashMultimap.create();
        map.put("damir", "test1");
        map.put("damir", "test2");
        map.put("damir", "test1");
        map.put("vandic", "test1");
        assertTrue(map.get("damir").size() == 2);
        assertTrue(map.get("vandic").size() == 1);
    }
}
