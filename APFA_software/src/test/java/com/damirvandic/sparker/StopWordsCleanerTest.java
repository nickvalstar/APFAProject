package com.damirvandic.sparker;

import com.damirvandic.sparker.blocking.tokenizers.StopWordsCleaner;
import org.junit.Assert;
import org.junit.Test;

public class StopWordsCleanerTest {

    @Test
    public void test() {
        StopWordsCleaner stopWordsCleaner = new StopWordsCleaner();
        Assert.assertEquals("damir vandic yes", stopWordsCleaner.clean("damir vandic your yes"));
    }

}
