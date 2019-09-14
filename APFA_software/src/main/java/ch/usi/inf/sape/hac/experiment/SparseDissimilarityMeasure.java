package ch.usi.inf.sape.hac.experiment;

import com.damirvandic.sparker.data.Pair;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class SparseDissimilarityMeasure<T> implements DissimilarityMeasure<T> {

    private final Map<Pair<T>, Double> map;

    public SparseDissimilarityMeasure(Map<Pair<T>, Double> map) {
        this.map = map;
    }

    public SparseDissimilarityMeasure(Iterable<Tuple2<Tuple2<T, T>, Double>> map) {
        this(convertMap(map));
    }

    private static <T> Map<Pair<T>, Double> convertMap(Iterable<Tuple2<Tuple2<T, T>, Double>> map) {
        Map<Pair<T>, Double> ret = new HashMap<>();
        for (Tuple2<Tuple2<T, T>, Double> e : map) {
            ret.put(new Pair<>(e._1()._1(), e._1()._2()), e._2());
        }
        return ret;
    }


    @Override
    public double computeDissimilarity(T o1, T o2) {
        Pair<T> p = new Pair<>(o1, o2);
        return (map.containsKey(p)) ? map.get(p) : 1.0;
    }

}
