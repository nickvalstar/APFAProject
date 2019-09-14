package com.damirvandic.sparker.students.group6;

import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.students.group6.UniformProductDescription.MyVectorBuilder;
import com.damirvandic.sparker.students.group6.UniformProductDescription.UniProductDesc;
import com.damirvandic.sparker.students.group6.UniformProductDescription.UniProductDescBuilder;
import com.damirvandic.sparker.students.group6.UniformProductDescription.UniformDescriptions;

import java.util.ArrayList;
import java.util.Set;

/**
 * Created by wkuipers on 02-10-14.
 */
public class LSHVectorBuilder {
    private ArrayList<Integer> indicesMissingValues;
    private ArrayList<Vector> vectors;

    public LSHVectorBuilder(Set<ProductDesc> products, double alpha, double beta, double gamma) {
//        String dsName = "TV's";
//        String dsPath = "./data/TVs-all-merged.json";
//
//        DataSet data = new DataSet(dsName,dsPath);
//        Set<ProductDesc> products = data.getProductDescriptions();

//        KeyValues kvs = new KeyValues(products);
//        TypeClassifier classifier = new TypeClassifier(kvs, 0.9);
//        classifier.printClassification();

        UniformDescriptions u = new UniformDescriptions(products, alpha, beta, gamma);

        UniProductDescBuilder builder = new UniProductDescBuilder(u, products);
        builder.build();

        Set<UniProductDesc> uniProductDescs = builder.getUniProductDescs();

        MyVectorBuilder vectorBuilder = new MyVectorBuilder(uniProductDescs, u, builder);
        vectorBuilder.buildVectors();
        Set<Vector> V = vectorBuilder.getVectors();

        indicesMissingValues = vectorBuilder.getIndicesMissingValues();

        vectors = new ArrayList<Vector>();
        for (Vector v : V) {
            vectors.add(v);
        }
        vectors = sortVectors(vectors);
    }

    private static ArrayList<Vector> sortVectors(ArrayList<Vector> a) {
        int nrProducts = a.size();

        ArrayList<Vector> c = new ArrayList<Vector>();
        for (int i = 0; i < nrProducts; i++) {
            c.add(new Vector(1));
        }
        for (int i = 0; i < nrProducts; i++) {
            c.set(a.get(i).getProductNumber() - 1, a.get(i));
        }
        return c;
    }

    public ArrayList<Vector> retrieveVectors() {
        return vectors;
    }

    public ArrayList<Integer> getIndicesMissingValues() {
        return indicesMissingValues;
    }
}
