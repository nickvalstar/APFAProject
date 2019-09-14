package com.damirvandic.sparker.students.group6.LSH;

import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.students.group6.HSLClustering;
import com.damirvandic.sparker.students.group6.Vector;

import java.util.*;

/**
 * Created by wkuipers on 29-10-14.
 */
public class TestLSH {
    public static void main(String[] args) {
        int nrOfVectors = 1000;
        int vectorLength = 1237;
        double p = 10 / 1000;

        double sigSize = 0.2;
        double t = 0.1;
        Random r = new Random();

        // init. vectors
        List<Vector> vectors = new ArrayList<Vector>();
        Vector v;
        for (int i = 0; i < nrOfVectors; i++) {
            v = new Vector(vectorLength, i + "");
            for (int j = 0; j < vectorLength; j++) {
                if (r.nextDouble() < p) {
                    v.set(j, 1);
                }
            }
            v.setProduct(new ProductDesc(i, "", "", "", new HashMap<String, String>()));
            vectors.add(v);
        }

        long t1 = System.currentTimeMillis();

//        System.out.println("Original vectors");
//        for (int i = 0; i<vectorLength; i++)
//        {
//            System.out.printf("%4d",i);
//            System.out.print("| ");
//            for (int k = 0; k<nrOfVectors; k++) {
//                System.out.print(vectors.get(k).get(i)+" ");
//            }
//            System.out.println();
//        }

        int sig_length = (int) (sigSize * vectorLength);
        MinHash minHash = new MinHash(sig_length, vectorLength);

        List<Signature> signatures = minHash.hash(vectors);


//        System.out.println("\nSignatures");
//        for (int i = 0; i<sig_length; i++)
//        {
//            System.out.printf("%4d",i);
//            System.out.print("| ");
//            for (int k = 0; k<nrOfVectors; k++) {
//                System.out.print(vectors.get(k).getSignature().get(i)+" ");
//            }
//            System.out.println();
//        }

        LocalitySensitiveHash LSH = new LocalitySensitiveHash(vectors, t);
        Map<Vector, Set<Vector>> lshOutput = LSH.performLSH();

        long t2 = System.currentTimeMillis();

        ArrayList<Vector> setje = new ArrayList<Vector>();
        for (int i = 0; i < vectors.size(); i++) {
            setje.add(vectors.get(i));
        }

        HSLClustering clust = new HSLClustering(lshOutput, 0.99, 4, setje, new ArrayList<Integer>(), 1);
        Set<Set<ProductDesc>> resultsss = clust.performClustering();
        System.out.println(resultsss.size());
        System.out.println("---");


//        System.out.println("\nNearest Neighbors for vectors");
//        for (Map.Entry<Vector,Set<Vector>> e: lshOutput.entrySet())
//        {
//            System.out.print("vector "+e.getKey().getKey()+": ");
//            for (Vector v1: e.getValue())
//            {
//                System.out.print(v1.getKey()+" | ");
//            }
//            System.out.println();
//        }
//
        System.out.println("\nRunning time: " + (t2 - t1) + " ms");
        System.out.println(resultsss.size());
    }

    public static void printArray(int[] a) {
        for (int x : a) {
            System.out.println(x);
        }
    }
}