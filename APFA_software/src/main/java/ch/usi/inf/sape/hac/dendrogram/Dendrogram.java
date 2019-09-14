/*
 * This file is licensed to You under the "Simplified BSD License".
 * You may not use this software except in compliance with the License. 
 * You may obtain a copy of the License at
 *
 * http://www.opensource.org/licenses/bsd-license.php
 * 
 * See the COPYRIGHT file distributed with this work for information
 * regarding copyright ownership.
 */
package ch.usi.inf.sape.hac.dendrogram;


import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A Dendrogram represents the results of hierachical agglomerative clustering.
 * The root represents a single cluster containing all observations.
 *
 * @author Matthias.Hauswirth@usi.ch
 */
public final class Dendrogram {

    private final DendrogramNode root;


    public Dendrogram(final DendrogramNode root) {
        this.root = root;
    }

    public DendrogramNode getRoot() {
        return root;
    }

    public void dump() {
        dumpNode("  ", root);
    }

    private void dumpNode(final String indent, final DendrogramNode node) {
        if (node == null) {
            System.out.println(indent + "<null>");
        } else if (node instanceof ObservationNode) {
            System.out.println(indent + "[" + node + "]");
        } else if (node instanceof MergeNode) {
            System.out.printf("%sM[%.3f]:\n", indent, ((MergeNode) node).getDissimilarity());

            dumpNode(indent + "  ", ((MergeNode) node).getLeft());
            dumpNode(indent + "  ", ((MergeNode) node).getRight());
        }
    }

    public Set<Set<Integer>> getClusters(double epsilon) {
        return new ClusterBuilder(epsilon).getClusters(root);
    }

    private static class ClusterBuilder {
        private final double epsilon;
        private final Set<Set<Integer>> ret;

        ClusterBuilder(double epsilon) {
            this.epsilon = epsilon;
            ret = new HashSet<>();
        }

        private Set<Set<Integer>> getClusters(DendrogramNode n) {
            traverse(n);
            return Collections.unmodifiableSet(ret);
        }

        private void traverse(DendrogramNode n) {
            if (n == null) return;
            if (n instanceof ObservationNode) { // add singleton
                HashSet<Integer> s = new HashSet<>();
                s.add(((ObservationNode) n).getObservation());
                ret.add(Collections.unmodifiableSet(s));
            } else { // it's a merge node
                MergeNode m = (MergeNode) n;
                if (m.getDissimilarity() > epsilon) { // split if d > epsilon
                    traverse(m.getLeft());
                    traverse(m.getRight());
                } else { // collect children
                    Set<Integer> children = new HashSet<>();
                    collect(m, children);
                    ret.add(Collections.unmodifiableSet(children));
                }
            }
        }

        private void collect(DendrogramNode n, Set<Integer> children) {
            if (n instanceof ObservationNode) {
                children.add(((ObservationNode) n).getObservation());
            } else {
                MergeNode m = (MergeNode) n;
                collect(m.getLeft(), children);
                collect(m.getRight(), children);
            }
        }

    }

}
