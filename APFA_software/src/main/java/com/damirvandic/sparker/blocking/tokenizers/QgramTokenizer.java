package com.damirvandic.sparker.blocking.tokenizers;

import com.damirvandic.sparker.blocking.core.Block;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import uk.ac.shef.wit.simmetrics.tokenisers.InterfaceTokeniser;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class QgramTokenizer extends AbstractCachingTokenizer<String> {

    private static final HashSet<Character> EXCLUDE_SET = new HashSet<>(Arrays.asList('-', '_', '=', '+', ',', '!', '@', '#', '$', '%', '^', '&', '*', '{', '}', '(', ')', '[', ']', '<', '>', '/', '\\', ';', '?', '|', '`', '~'));
    private final StopWordsCleaner stopWordsCleaner;
    private final InterfaceTokeniser tokeniser;
    private final String name;

    public QgramTokenizer(InterfaceTokeniser tokeniser, String name) {
        this(Sets.<String>newHashSet(), tokeniser, name);
    }

    public QgramTokenizer(Set<String> extratopWords, InterfaceTokeniser tokeniser, String name) {
        super(0, 0); // no cache
        Preconditions.checkNotNull(extratopWords);
        this.name = name;
        this.stopWordsCleaner = new StopWordsCleaner(extratopWords);
        this.tokeniser = tokeniser;
    }

    @Override
    protected Set<Block> extractBlocks(String value) {
        value = value.toLowerCase();
        value = stopWordsCleaner.clean(value);
        StringBuilder builder = new StringBuilder();
        int n = value.length();
        for (int i = 0; i < n; i++) {
            char c = value.charAt(i);
            if (!EXCLUDE_SET.contains(new Character(c))) {
                builder.append(c);
            }
        }
        return toBlocks(this.tokeniser.tokenizeToArrayList(builder.toString()));
    }

    @Override
    public String componentID() {
        return this.name;
    }
}
