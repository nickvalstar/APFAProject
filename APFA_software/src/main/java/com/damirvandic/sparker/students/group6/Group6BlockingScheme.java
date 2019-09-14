package com.damirvandic.sparker.students.group6;

import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.blocking.core.BlocksMapping;
import com.damirvandic.sparker.blocking.schemes.BlockingScheme;
import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.students.group6.LSH.LocalitySensitiveHash;
import com.damirvandic.sparker.students.group6.LSH.MinHash;

import java.util.*;

public class Group6BlockingScheme implements BlockingScheme<ProductDesc> {
    private final double lshThreshold;
    private final double minHashSize;

    public Group6BlockingScheme(double lshThreshold, double minHashSize) {
        this.lshThreshold = lshThreshold;
        this.minHashSize = minHashSize;
    }

    @Override
    public BlocksMapping<ProductDesc> getBlocks(Set<ProductDesc> data) {
        BlocksMapping.Builder<ProductDesc> builder = BlocksMapping.builder();
        Map<ProductDesc, Set<ProductDesc>> buckets = computeBlocks(data);
        int i = 1;
        for (ProductDesc p1 : buckets.keySet()) {
            Block b = new Block("block" + i);
            builder.add(p1, b);
            for (ProductDesc p2 : buckets.get(p1)) {
                builder.add(p2, b);
            }
            i++;
        }
        return builder.build();
    }

    @Override
    public void clearCache() {
        // we are not using cache so ignore this method
    }

    @Override
    public String componentID() {
        return String.format("g6_blocking_%3f_%3f", minHashSize, lshThreshold);
    }

    @Override
    public String toString() {
        return componentID();
    }

    private Map<ProductDesc, Set<ProductDesc>> computeBlocks(Set<ProductDesc> products) {
        long t1, t2, t3, t4, t5;

        t1 = System.currentTimeMillis();
        // create vectors from data
        //LSHVectorBuilder vectorBuilder = new LSHVectorBuilder(products, alpha, beta, gamma);
        LSHVectorsFromModelWords vectorBuilder = new LSHVectorsFromModelWords(products);
        ArrayList<Vector> vectors = vectorBuilder.retrieveVectors();
        t2 = System.currentTimeMillis();

        int vectorLength = vectors.get(0).getDimensions();

        t3 = System.currentTimeMillis();
        // generate minHash signatures and add them to vector objects
        int signatureLength = (int) (vectorLength * minHashSize);
        MinHash minHash = new MinHash(signatureLength, vectorLength);
        minHash.hash(vectors);

        LocalitySensitiveHash lsh = new LocalitySensitiveHash(vectors, lshThreshold);
        Map<Vector, Set<Vector>> nearestNeighbors = lsh.performLSH();
        t4 = System.currentTimeMillis();

        Map<ProductDesc, Set<ProductDesc>> output = new HashMap<>();
        Set<ProductDesc> P;
        for (Vector v1 : nearestNeighbors.keySet()) {
            P = new HashSet<ProductDesc>();
            output.put(v1.getProduct(), P);
            for (Vector v2 : nearestNeighbors.get(v1)) {
                P.add(v2.getProduct());
            }
        }

        return output;
    }
}
