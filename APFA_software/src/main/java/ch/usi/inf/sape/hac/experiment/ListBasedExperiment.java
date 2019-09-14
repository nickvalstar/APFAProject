package ch.usi.inf.sape.hac.experiment;

import java.util.List;

public class ListBasedExperiment<T> implements Experiment<T> {
    private final List<T> list;

    public ListBasedExperiment(List<T> list) {
        this.list = list;
    }

    @Override
    public int getNumberOfObservations() {
        return list.size();
    }

    @Override
    public T getObservation(int index) {
        return list.get(index);
    }
}
