package com.damirvandic.sparker;

import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.blocking.core.BlockingEvaluator;
import com.damirvandic.sparker.blocking.core.BlockingResults;
import com.damirvandic.sparker.blocking.schemes.AbstractBlockingScheme;
import com.damirvandic.sparker.blocking.schemes.BlockingScheme;
import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.data.DataSet;
import com.damirvandic.sparker.data.Reader;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class BlockingEvalTest {

    @Test
    public void test() {
        BlockingScheme<ProductDesc> s1 = new AbstractBlockingScheme<ProductDesc>() {
            public Set<Block> getBlocks(ProductDesc p) {
                return ImmutableSet.of(new Block("DUMMY"));
            }

            @Override
            public String componentID() {
                return "dummy";
            }
        };
        DataSet dataSet = Reader.readDataSet("TV's", "./hdfs/TVs-micro.json");
        BlockingEvaluator evaluator = new BlockingEvaluator(dataSet, 10);
        BlockingResults results = evaluator.run(ImmutableSet.of(s1));
        double f1 = results.resultsTable.row(0).get(s1).getClusterResults().f1();
        Assert.assertEquals(1.0, f1, 0.00001);
    }
}
