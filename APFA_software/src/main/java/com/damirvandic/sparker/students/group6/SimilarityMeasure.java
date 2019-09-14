package com.damirvandic.sparker.students.group6;

import java.util.ArrayList;

/**
 * Created by Gerhard on 27-9-2014.
 * ;
 * Class with a static method that calculates the similarity of two products, based on their uniform product representation.
 */
public class SimilarityMeasure {


    //incorrect method! Input parameters must be two product descriptions. Output must be the similarity on interval [0,1]
    public static double calculateCosineSimilarity(Vector a, Vector b, ArrayList<Integer> indicesMissing) {
        double aDOTb = 0;
        double lengthA = 0;
        double lengthB = 0;

        double ai, bi;

        for (int i = 0; i < a.getDimensions(); i++) {
            ai = a.get(i);
            bi = b.get(i);

            aDOTb = aDOTb + ai * bi;

            lengthA = lengthA + ai * ai;
            lengthB = lengthB + bi * bi;
        }
        for (int i = 0; i < indicesMissing.size(); i++) {
            int index = indicesMissing.get(i);
            if (a.get(index) != 0 || b.get(index) != 0) {
                lengthA = lengthA - 1;
                lengthB = lengthB - 1;
                if (a.get(index) != 0 && b.get(index) != 0) {
                    aDOTb = aDOTb - 1;
                }
            }
        }


        if (lengthA == 0 || lengthB == 0) {
            // System.out.println("fail");
            return 0;
        }
        double similarity = aDOTb / (Math.sqrt(lengthA) * Math.sqrt(lengthB));
        return similarity;

    }

}