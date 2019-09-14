package com.damirvandic.sparker.blocking.schemes;

import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.blocking.core.BlocksMapping;
import com.damirvandic.sparker.blocking.util.BlockingUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class BlockingSchemes {
    public static <T> BlockingScheme<T> union(BlockingScheme<T> a, BlockingScheme<T> b) {
        return union(Arrays.asList(a, b));
    }

    public static <T> BlockingScheme<T> union(Collection<BlockingScheme<T>> schemes) {
        return new UnionScheme<>(schemes);
    }

    public static <T> BlockingScheme<T> product(BlockingScheme<T> a, BlockingScheme<T> b) {
        return new ProductScheme<>(a, b);
    }

    private static class UnionScheme<T> implements BlockingScheme<T> {
        private final Logger log = LoggerFactory.getLogger(getClass());
        private final Collection<BlockingScheme<T>> schemes;

        private UnionScheme(Collection<BlockingScheme<T>> schemes) {
            this.schemes = schemes;
        }

        @Override
        public String componentID() {
            return String.format("%s", BlockingUtils.aggregateComponentIDs(schemes, "_+_"));
        }

        @Override
        public String toString() {
            return componentID();
        }

        @Override
        public BlocksMapping<T> getBlocks(Set<T> data) {
            BlocksMapping.Builder<T> builder = BlocksMapping.builder();
            for (BlockingScheme<T> scheme : schemes) {
                BlocksMapping<T> blocks = scheme.getBlocks(data);
                builder.addAll(blocks);
            }
            BlocksMapping<T> ret = builder.build();
            log.debug("Scheme '{}' produced {} blocks for {} entities", componentID(), ret.blockCount(), data.size());
            return ret;
        }

        @Override
        public void clearCache() {
            for (BlockingScheme<T> scheme : schemes) {
                log.info("Clearing cache for schemes {}", Joiner.on(", ").join(schemes));
                scheme.clearCache();
            }
        }
    }

    private static class ProductScheme<T> implements BlockingScheme<T> {
        private final Logger log = LoggerFactory.getLogger(getClass());
        private final BlockingScheme<T> a;
        private final BlockingScheme<T> b;

        private ProductScheme(BlockingScheme<T> a, BlockingScheme<T> b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public String componentID() {
            return String.format("%s_*_%s", a, b);
        }

        @Override
        public String toString() {
            return componentID();
        }

        @Override
        public BlocksMapping<T> getBlocks(Set<T> data) {
            BlocksMapping.Builder<T> builder = BlocksMapping.builder();
            BlocksMapping<T> phaseA = a.getBlocks(data);
            for (Block block : phaseA.blockToEntities.keySet()) {
                ImmutableCollection<T> group = phaseA.blockToEntities.get(block);
                BlocksMapping<T> phaseB = b.getBlocks(new HashSet<>(group));
                for (Map.Entry<Block, T> entry : phaseB.blockToEntities.entries()) {
                    Block b = new Block(block.getStrID(), entry.getKey());
                    builder.add(entry.getValue(), b);
                }
            }
            BlocksMapping<T> ret = builder.build();
            log.debug("Scheme '{}' produced {} blocks for {} entities", componentID(), ret.blockCount(), data.size());
            return ret;
        }

        @Override
        public void clearCache() {
            log.info("Clearing cache for schemes {} and {}", a, b);
            a.clearCache();
            b.clearCache();
        }
    }
}
