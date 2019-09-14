package com.damirvandic.sparker;

import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.blocking.tokenizers.CleaningTokenizer;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class CleaningTokenizerTest {
    @Test
    public void test() {
        CleaningTokenizer tokenizer = new CleaningTokenizer();
        Set<Block> tokens = tokenizer.tokenize("this is_ a test bestbuy.com thenerds.com");
        Assert.assertEquals(ImmutableSet.of(
                new Block("this"),
                new Block("is"),
                new Block("a"),
                new Block("test")), tokens);
    }
}
