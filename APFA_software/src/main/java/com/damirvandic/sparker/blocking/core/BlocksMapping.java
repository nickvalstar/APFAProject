package com.damirvandic.sparker.blocking.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import java.util.Collection;
import java.util.Map;

public class BlocksMapping<T> {
    public final ImmutableMultimap<Block, T> blockToEntities;
    public final ImmutableMultimap<T, Block> entitiesToBlocks;

    private BlocksMapping(Multimap<Block, T> blockToEntities, Multimap<T, Block> entitiesToBlocks) {
        Preconditions.checkNotNull(blockToEntities);
        Preconditions.checkNotNull(entitiesToBlocks);
        Preconditions.checkArgument(blockToEntities.size() == entitiesToBlocks.size());
        this.blockToEntities = ImmutableMultimap.copyOf(blockToEntities);
        this.entitiesToBlocks = ImmutableMultimap.copyOf(entitiesToBlocks);
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlocksMapping that = (BlocksMapping) o;
        if (!blockToEntities.equals(that.blockToEntities)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return blockToEntities.hashCode();
    }

    public int blockCount() {
        return blockToEntities.keySet().size();
    }

    public Collection<Collection<T>> blocksAsCollection() {
        return blockToEntities.asMap().values();
    }

    public Map<T, Collection<Block>> entities() {
        return entitiesToBlocks.asMap();
    }

    public static class Builder<T> {
        private final SetMultimap<Block, T> blockToEntities;
        private final SetMultimap<T, Block> entitiesToBlocks;

        Builder() {
            blockToEntities = HashMultimap.create();
            entitiesToBlocks = HashMultimap.create();
        }

        public Builder add(T entity, Block block) {
            blockToEntities.put(block, entity);
            entitiesToBlocks.put(entity, block);
            return this;
        }

        public BlocksMapping<T> build() {
            return new BlocksMapping<>(blockToEntities, entitiesToBlocks);
        }

        public void addAll(BlocksMapping<T> blocks) {
            blockToEntities.putAll(blocks.blockToEntities);
            entitiesToBlocks.putAll(blocks.entitiesToBlocks);
        }
    }
}
