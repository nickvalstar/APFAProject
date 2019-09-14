package com.damirvandic.sparker.blocking.util;

import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.blocking.core.Component;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class BlockingUtils {

    public static final String MERGE_SEP = "|_|";
    private static final Function<Component, String> COMPONENT_STRING_FUNCTION = new Function<Component, String>() {
        @Nullable
        @Override
        public String apply(Component c) {
            return c.componentID();
        }
    };

    public static Block of(String name) {
        return new Block(name);
    }

    public static Block newBlockFrom(Collection<Block> newBlocks) {
        List<Block> list = new ArrayList<>(newBlocks);
        Collections.sort(list);
        return new Block(Joiner.on(MERGE_SEP).join(list));
    }


    public static String aggregateComponentIDs(Collection<? extends Component> components) {
        return aggregateComponentIDs(components, ".");
    }

    public static String aggregateComponentIDs(Collection<? extends Component> components, String sep) {
        return Joiner.on(sep).join(Iterables.transform(components, COMPONENT_STRING_FUNCTION));
    }
}
