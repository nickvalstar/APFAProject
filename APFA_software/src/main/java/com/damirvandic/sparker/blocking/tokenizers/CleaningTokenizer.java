package com.damirvandic.sparker.blocking.tokenizers;

import com.damirvandic.sparker.blocking.core.Block;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class CleaningTokenizer extends AbstractCachingTokenizer<String> {

    private static final HashSet<Character> EXCLUDE_SET = new HashSet<>(Arrays.asList('-', '_', '=', '+', ',', '!', '@', '#', '$', '%', '^', '&', '*', '{', '}', '(', ')', '[', ']', '<', '>', '/', '\\', ';', '?', '|', '`', '~'));
    private final StopWordsCleaner stopWordsCleaner;

    public CleaningTokenizer() {
        this(Sets.<String>newHashSet());
    }

    public CleaningTokenizer(Set<String> extratopWords) {
        super(0, 0); // no cache
        Preconditions.checkNotNull(extratopWords);
        this.stopWordsCleaner = new StopWordsCleaner(extratopWords);
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
        return toBlocks(builder.toString().split(" "));
    }

    @Override
    public String componentID() {
        return "cl";
    }
}
