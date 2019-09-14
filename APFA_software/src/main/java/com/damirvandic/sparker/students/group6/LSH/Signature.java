package com.damirvandic.sparker.students.group6.LSH;

import com.damirvandic.sparker.students.group6.Vector;

/**
 * Created by wkuipers on 01-11-14.
 */
public class Signature {
    private int length;
    private int[] array;
    private Vector v;

    public Signature(int dim, Vector vector) {
        length = dim;
        array = new int[dim];
        v = vector;
    }

    public void set(int index, int value) {
        array[index] = value;
    }

    public int get(int index) {
        return array[index];
    }

    public int getLength() {
        return length;
    }

    public Vector getVector() {
        return v;
    }
}
