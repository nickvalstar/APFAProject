package com.damirvandic.sparker.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class ConnectedComponentsFinder<T> {

    private final Multimap<Vertex<T>, Vertex<T>> E;
    private final Map<T, Vertex<T>> V;

    public ConnectedComponentsFinder(Set<Pair<T, T>> edges) {
        this.V = createVertexMap(edges);
        this.E = processEdges(edges);
    }

    public Set<Set<T>> findClusters() {
        Set<Set<T>> ret = new HashSet<>();
        Set<T> currentCluster;
        for (Vertex<T> v : V.values()) {
            currentCluster = new HashSet<>();
            if (!v.visited) { // already put in a cluster
                traverseCluster(currentCluster, v);
            }
            ret.add(currentCluster);
        }
        return ret;
    }

    private void traverseCluster(Set<T> cluster, Vertex<T> v) {
        cluster.add(v.data);
        v.visited = true;
        for (Vertex<T> w : E.get(v)) { // add vertices from neighbours
            if (!w.visited) {
                traverseCluster(cluster, w);
            }
        }
    }


    private Map<T, Vertex<T>> createVertexMap(Set<Pair<T, T>> edges) {
        Map<T, Vertex<T>> ret = new HashMap<>();
        for (Pair<T, T> e : edges) {
            addToMap(ret, e.getLeft());
            addToMap(ret, e.getRight());
        }
        return ret;
    }

    private void addToMap(Map<T, Vertex<T>> ret, T obj) {
        if (!ret.containsKey(obj)) {
            ret.put(obj, new Vertex<>(obj));
        }
    }

    private Multimap<Vertex<T>, Vertex<T>> processEdges(Set<Pair<T, T>> edges) {
        Multimap<Vertex<T>, Vertex<T>> ret = HashMultimap.create();
        for (Pair<T, T> e : edges) {
            Vertex left = V.get(e.getLeft());
            Vertex right = V.get(e.getRight());
            ret.put(left, right);
            ret.put(right, left);
        }
        return ret;
    }

    private static class Vertex<T> {
        public final T data;
        public boolean visited;

        public Vertex(T data) {
            Preconditions.checkNotNull(data);
            this.data = data;
            this.visited = false;
        }

        @Override
        public int hashCode() {
            return data.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return !(obj == null || !(obj instanceof Vertex)) && Objects.equals(((Vertex) obj).data, this.data);
        }

        @Override
        public String toString() {
            return String.format("V[%s]", data.toString());
        }
    }
}
