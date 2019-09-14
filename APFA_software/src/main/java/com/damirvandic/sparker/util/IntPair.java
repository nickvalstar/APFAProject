package com.damirvandic.sparker.util;

public class IntPair implements java.io.Serializable {
    public final int id_a, id_b;

    public IntPair(int id_a, int id_b) {
        this.id_a = id_a < id_b ? id_a : id_b;
        this.id_b = id_a < id_b ? id_b : id_a;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof IntPair)) return false;
        IntPair o = (IntPair) obj;
        return o.id_a == id_a && o.id_b == id_b;
    }

    @Override
    public int hashCode() {
        int res = 17;
        res = res * 31 + id_a;
        res = res * 31 + id_b;
        return res;
    }
}
