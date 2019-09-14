package com.damirvandic.sparker.blocking.tokenizers;

import com.damirvandic.sparker.blocking.core.Block;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ModelWordsTokenizer extends AbstractCachingTokenizer<String> {
    public static final Pattern PRE_PROCESS_PATTERN = Pattern.compile("/ +|\\, +|\\. +|\\.$|\\(|\\)");
    private final Pattern pattern = getMWPattern();
    private final StopWordsCleaner stopWordsCleaner;

    public ModelWordsTokenizer() {
        this(Sets.<String>newHashSet());
    }

    public ModelWordsTokenizer(Set<String> extraStopWords) {
        super(0, 0); // no cache
        Preconditions.checkNotNull(extraStopWords);
        this.stopWordsCleaner = new StopWordsCleaner(extraStopWords);
    }

    private static Pattern getMWPattern() {
        String regex = "([a-zA-Z0-9]*(([0-9]+[^0-9^,^ ]+)|([^0-9^,^ ]+[0-9]+))[a-zA-Z0-9\"]*)";
        Pattern p = Pattern.compile(regex);
        return p;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ModelWordsTokenizer that = (ModelWordsTokenizer) o;

        if (!pattern.equals(that.pattern)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return pattern.hashCode();
    }

    @Override
    protected Set<Block> extractBlocks(String value) {
        value = stopWordsCleaner.clean(value);
        value = preProcess(value);
        Matcher m = pattern.matcher(value);
        Set<Block> c = new HashSet<>();
        while (m.find()) {
            c.add(new Block(m.group(1)));
        }
        return c;
    }

    private String preProcess(String str) {
        //remove slash, comma, and period at end of word (or period at end of whole string)
        return PRE_PROCESS_PATTERN.matcher(str).replaceAll(" ").toLowerCase().trim();
    }

    @Override
    public String componentID() {
        return "mw";
    }
}
