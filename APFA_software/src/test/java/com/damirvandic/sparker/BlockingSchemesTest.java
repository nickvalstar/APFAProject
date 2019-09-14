package com.damirvandic.sparker;

import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.blocking.core.BlocksMapping;
import com.damirvandic.sparker.blocking.schemes.BlockingScheme;
import com.damirvandic.sparker.blocking.schemes.BlockingSchemes;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class BlockingSchemesTest {
    public static final Set<String> TEST_DATA = ImmutableSet.of("a1", "a2", "a3", "b1", "b2", "b3", "c1", "c2", "c3");

    BlockingScheme<String> s1 = new BlockingScheme<String>() {
        @Override
        public BlocksMapping<String> getBlocks(Set<String> data) {
            BlocksMapping.Builder<String> builder = BlocksMapping.builder();
            builder.add("a1", new Block("X"));
            builder.add("a2", new Block("X"));
            builder.add("b1", new Block("X"));

            builder.add("a2", new Block("Y"));
            builder.add("a3", new Block("Y"));
            builder.add("b2", new Block("Y"));
            builder.add("b3", new Block("Y"));


            builder.add("c1", new Block("Z"));
            builder.add("c2", new Block("Z"));
            builder.add("c3", new Block("Z"));
            return builder.build();
        }

        @Override
        public void clearCache() {

        }

        @Override
        public String componentID() {
            return "dummy";
        }
    };

    BlockingScheme<String> s2 = new BlockingScheme<String>() {
        @Override
        public BlocksMapping<String> getBlocks(Set<String> data) {
            BlocksMapping.Builder<String> builder = BlocksMapping.builder();
            for (String e : data) {
                if (e.startsWith("a")) {
                    builder.add(e, new Block("b"));
                    builder.add(e, new Block("c"));
                } else if (e.startsWith("b")) {
                    builder.add(e, new Block("a"));
                    builder.add(e, new Block("c"));
                } else if (e.startsWith("c")) {
                    builder.add(e, new Block("a"));
                    builder.add(e, new Block("b"));
                }
            }
            return builder.build();
        }

        @Override
        public void clearCache() {

        }

        @Override
        public String componentID() {
            return "dummy";
        }
    };

    public static Block b(String name) {
        return new Block(name);
    }

    @Test
    public void testBasic() {
        BlocksMapping<String> blocks_s1 = s1.getBlocks(TEST_DATA);
        assertBlocks("a1", blocks_s1, b("X"));
        assertBlocks("a2", blocks_s1, b("X"), b("Y"));
        BlocksMapping<String> blocks_s2 = s2.getBlocks(TEST_DATA);
        assertBlocks("a1", blocks_s2, b("b"), b("c"));
        assertBlocks("a2", blocks_s2, b("b"), b("c"));
        assertBlocks("a3", blocks_s2, b("b"), b("c"));
        assertBlocks("b1", blocks_s2, b("a"), b("c"));
        assertBlocks("b2", blocks_s2, b("a"), b("c"));
        assertBlocks("b3", blocks_s2, b("a"), b("c"));
        assertBlocks("c1", blocks_s2, b("a"), b("b"));
        assertBlocks("c2", blocks_s2, b("a"), b("b"));
        assertBlocks("c3", blocks_s2, b("a"), b("b"));
    }


    private void assertBlocks(String entity, BlocksMapping<String> blocksMap, Block... blocksExpArray) {
        List<Block> blocksExpected = Arrays.asList(blocksExpArray);
        List<Block> blocksActual = new ArrayList<>(blocksMap.entitiesToBlocks.get(entity));
        Collections.sort(blocksActual);
        Collections.sort(blocksExpected);
        Assert.assertEquals(blocksExpected, blocksActual);

    }


}
