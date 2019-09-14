package com.damirvandic.sparker.blocking.core;

public class Block implements Comparable<Block> {
    private final String strID;
    private final int hash;

    public Block(String strID) {
        this.strID = strID;
        this.hash = strID.hashCode();
    }

    public Block(String prefix, Block b) {
        this.strID = prefix + "_" + b.strID;
        this.hash = strID.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Block block = (Block) o;

        if (!strID.equals(block.strID)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public String getStrID() {
        return strID;
    }

    @Override
    public String toString() {
        return strID;
    }

    @Override
    public int compareTo(Block o) {
        return strID.compareTo(o.strID);
    }
}
