package com.damirvandic.sparker;

import ch.usi.inf.sape.hac.HierarchicalAgglomerativeClusterer;
import ch.usi.inf.sape.hac.agglomeration.AgglomerationMethod;
import ch.usi.inf.sape.hac.agglomeration.SingleLinkage;
import ch.usi.inf.sape.hac.dendrogram.Dendrogram;
import ch.usi.inf.sape.hac.dendrogram.DendrogramBuilder;
import ch.usi.inf.sape.hac.experiment.DissimilarityMeasure;
import ch.usi.inf.sape.hac.experiment.Experiment;
import ch.usi.inf.sape.hac.experiment.ListBasedExperiment;
import ch.usi.inf.sape.hac.experiment.SparseDissimilarityMeasure;
import org.junit.Test;
import scala.Tuple2;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HacTesting {
    @Test
    public void basicTest() throws Exception {
        List<Prod> list = getSampleData();
        Experiment<Prod> experiment = new ListBasedExperiment<>(list);
        DissimilarityMeasure<Prod> dissimilarityMeasure = new SparseDissimilarityMeasure<>(getDissimilarities());
        AgglomerationMethod agglomerationMethod = new SingleLinkage();
        DendrogramBuilder dendrogramBuilder = new DendrogramBuilder(experiment.getNumberOfObservations());
        HierarchicalAgglomerativeClusterer<Prod> clusterer = new HierarchicalAgglomerativeClusterer<>(experiment, dissimilarityMeasure, agglomerationMethod);
        clusterer.cluster(dendrogramBuilder);
        Dendrogram dendrogram = dendrogramBuilder.getDendrogram();

        Set<Set<Integer>> clusters1 = dendrogram.getClusters(0.025);
        assertEquals(clusters1.size(), 5);
        assertTrue(clusters1.contains(s(0)));
        assertTrue(clusters1.contains(s(1, 2)));
        assertTrue(clusters1.contains(s(3)));
        assertTrue(clusters1.contains(s(4)));
        assertTrue(clusters1.contains(s(5)));

        Set<Set<Integer>> clusters2 = dendrogram.getClusters(0.3);
        assertEquals(clusters2.size(), 3);
        assertTrue(clusters2.contains(s(0)));
        assertTrue(clusters2.contains(s(1, 2)));
        assertTrue(clusters2.contains(s(3, 4, 5)));

        Set<Set<Integer>> clusters3 = dendrogram.getClusters(0.4);
        assertEquals(clusters3.size(), 2);
        assertTrue(clusters3.contains(s(0)));
        assertTrue(clusters3.contains(s(1, 2, 3, 4, 5)));

        Set<Set<Integer>> clusters4 = dendrogram.getClusters(0.5);
        assertEquals(clusters4.size(), 2);
        assertTrue(clusters4.contains(s(0)));
        assertTrue(clusters4.contains(s(1, 2, 3, 4, 5)));

        Set<Set<Integer>> clusters5 = dendrogram.getClusters(0.9);
        assertEquals(clusters5.size(), 1);
        assertTrue(clusters5.contains(s(0, 1, 2, 3, 4, 5)));
    }

    private Set<Integer> s(Integer... i) {
        return new HashSet<>(Arrays.asList(i));
    }

    private ArrayList<Prod> getSampleData() {
        ArrayList<Prod> ret = new ArrayList<>();
        ret.add(new Prod("A"));
        ret.add(new Prod("B"));
        ret.add(new Prod("C"));
        ret.add(new Prod("D"));
        ret.add(new Prod("E"));
        ret.add(new Prod("F"));
        return ret;
    }

    public List<Tuple2<Tuple2<Prod, Prod>, Double>> getDissimilarities() {
        List<Tuple2<Tuple2<Prod, Prod>, Double>> ret = new ArrayList<>();
        ret.add(d("A", "B", .9));
        ret.add(d("A", "C", .95));
        ret.add(d("A", "D", .89));
        ret.add(d("A", "E", .91));
        ret.add(d("A", "F", .95));
        ret.add(d("B", "C", .02));
        ret.add(d("B", "D", .4));
        ret.add(d("B", "E", .4));
        ret.add(d("B", "F", .7));
        ret.add(d("C", "D", .4));
        ret.add(d("C", "E", .4));
        ret.add(d("C", "F", .7));
        ret.add(d("D", "E", .03));
        ret.add(d("D", "F", .1));
        ret.add(d("E", "F", .1));
        return ret;
    }

    private Tuple2<Tuple2<Prod, Prod>, Double> d(String a, String b, Double d) {
        return new Tuple2<>(new Tuple2<>(new Prod(a), new Prod(b)), d);
    }

    static class Prod {
        private final String title;

        Prod(String title) {
            if (title == null) throw new IllegalArgumentException();
            this.title = title;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Prod prod = (Prod) o;

            if (!title.equals(prod.title)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return title.hashCode();
        }
    }

}

