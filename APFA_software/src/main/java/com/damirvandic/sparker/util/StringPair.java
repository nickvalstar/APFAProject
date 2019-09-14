package com.damirvandic.sparker.util;

public class StringPair implements java.io.Serializable {
    public final String id_a, id_b;

    public StringPair(String id_a, String id_b) {
        if (id_a == null || id_b == null) throw new IllegalArgumentException();
        this.id_a = id_a.compareTo(id_b) < 0 ? id_a : id_b;
        this.id_b = id_a.compareTo(id_b) < 0 ? id_b : id_a;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof StringPair)) return false;
        StringPair o = (StringPair) obj;
        return o.id_a.equals(id_a) && o.id_b.equals(id_b);
    }

    @Override
    public int hashCode() {
        int res = 17;
        res = res * 31 + id_a.hashCode();
        res = res * 31 + id_b.hashCode();
        return res;
    }
}
