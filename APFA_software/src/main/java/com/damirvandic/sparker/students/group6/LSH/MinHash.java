package com.damirvandic.sparker.students.group6.LSH;


import com.damirvandic.sparker.students.group6.Vector;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Created by wkuipers on 28-10-14.
 */
public class MinHash {
    private int nrHashFunctions;
    private int vectorLength;
    private Random random;

    public MinHash(int hashFunctions, int length) {
        nrHashFunctions = hashFunctions;
        vectorLength = length;
        random = new Random();
    }

    /**
     * Creates signatures of all vectors
     *
     * @param vectors input vectors
     * @return signatures
     */
    public List<Signature> hash(List<Vector> vectors) {
        int nrVectors = vectors.size();

        // initialize signatures
        List<Signature> signatures = new ArrayList<Signature>(nrVectors);
        for (int i = 0; i < nrVectors; i++) {
            signatures.add(i, new Signature(nrHashFunctions, vectors.get(i)));
        }


        int[] perm;
        for (int k = 0; k < nrHashFunctions; k++) {
            perm = getPermutation(vectorLength);
            //TestLSH.printArray(perm);
            //System.out.println();

            for (int v = 0; v < nrVectors; v++) {
                // find first occurence of 1;
                for (int i = 0; i < perm.length; i++) {
                    if (vectors.get(v).get(perm[i]) == 1) {
                        signatures.get(v).set(k, i);
                        break;
                    }
                }
            }
        }

        // add signatures to vector objects
        for (int i = 0; i < vectors.size(); i++) {
            vectors.get(i).setSignature(signatures.get(i));
        }

        return signatures;
    }

    public int[] getPermutation(int n) {
        int[] result = new int[n];
        LinkedList<Integer> array = new LinkedList<Integer>();
        for (int i = 0; i < n; i++) {
            array.add(i, i);
        }

        int r;
        for (int j = 0; j < n; j++) {
            r = random.nextInt(array.size());
            result[j] = array.get(r);
            array.remove(r);
        }
        return result;
    }
}
