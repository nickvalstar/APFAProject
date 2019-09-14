package com.damirvandic.sparker.marnix;

import com.damirvandic.sparker.core.ClusteringProcedure;
import com.damirvandic.sparker.core.Clusters;
import com.damirvandic.sparker.core.ClustersBuilder;
import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.util.IntPair;
import edu.ucla.sspace.clustering.*;
import edu.ucla.sspace.matrix.ArrayMatrix;
import edu.ucla.sspace.matrix.Matrix;
import edu.ucla.sspace.util.Generator;
import gnu.trove.map.TIntObjectMap;

import java.util.*;


/**
 * Created by Marnix on 16-12-2014.
 */
public class SpectralClusteringProcedure implements ClusteringProcedure {


    @Override
    public Clusters createClusters(Map<String, Object> conf, Map<IntPair, Double> similarities, TIntObjectMap<ProductDesc> productIDIndexMap) {
        Set<ProductDesc> products = new HashSet<>(productIDIndexMap.valueCollection());
        ClustersBuilder clustersBuilder = new ClustersBuilder();
        Set<Set<ProductDesc>> clusterSets = new HashSet<>();
        double alpha = (double) conf.get("alpha");
        String scMethod = (String) conf.get("method");
        boolean useWebShop = (boolean) conf.get("useWebShop");



        SimMatrix sims = createSimilarityMatrix(similarities,useWebShop,products);
        Matrix simmat = sims.getSimMat();
        //rowColIDs is an ArrayList containing the product ID corresponding to each row/column.
        Integer[] rowColIDs = sims.getRowColIDs();
//        Generator<SpectralCut> cutterGenerator;

        Assignments clusterAssignments;
        if (scMethod.equals("recursive")) {
            CKVWSpectralClustering03 sc = new CKVWSpectralClustering03();
            clusterAssignments = sc.cluster(simmat, new Properties());
        }else if (scMethod.equals("divmerge")) {
            CKVWSpectralClustering06 sc = new CKVWSpectralClustering06();
            clusterAssignments = sc.cluster(simmat, new Properties());
        }else{
            System.out.println("Unknown method specified, using divide-and-merge method instead.");
            CKVWSpectralClustering06 sc = new CKVWSpectralClustering06();
            clusterAssignments = sc.cluster(simmat, new Properties());
        }





//        SpectralClustering sc = SpectralClustering(alpha, new CKVWSpectralClustering03.SpectralCutGenerator());
//        Assignment[] clusterAssignments = sc.cluster(simmat);

//        ArrayList<Integer> clusterNumbers = new ArrayList<>();
        Map<Integer, Set<ProductDesc>> clustersMap = new HashMap<>();
        Set<ProductDesc> temp = new HashSet<>();
        Map<Integer, ProductDesc> idProdMap = mapIDsToProds(products);
        for (int i = 0; i < clusterAssignments.size(); i++) {
            for (int j = 0; j < clusterAssignments.get(i).length(); j++) {
                if (clustersMap.keySet().contains(clusterAssignments.get(i).assignments()[j])) {
                    temp = clustersMap.get(clusterAssignments.get(i).assignments()[j]);
                    temp.add(idProdMap.get(rowColIDs[i]));
                    clustersMap.put(clusterAssignments.get(i).assignments()[j], temp);
                } else {
                    temp = new HashSet<>();
                    temp.add(idProdMap.get(rowColIDs[i]));
                    clustersMap.put(clusterAssignments.get(i).assignments()[j], temp);
                }
            }
        }

        clusterSets = new HashSet<>(clustersMap.values());

        return clustersBuilder.fromSets(clusterSets);
    }

    @Override
    public String componentID() {
        return "scp"; // name of method (scp = SpectralClusteringProcedure)
    }


    public SimMatrix createSimilarityMatrix(Map<IntPair, Double> similarities, boolean useWebShop, Set<ProductDesc> prods){
        //Fill up rowColIDs: an ArrayList containing the product ID corresponding to each row/column.
        ArrayList<Integer> rowColIDs = new ArrayList<>();
        for (IntPair ip : similarities.keySet()) {
            if (!rowColIDs.contains(ip.id_a)) {
                rowColIDs.add(ip.id_a);
            }
            if (!rowColIDs.contains(ip.id_b)) {
                rowColIDs.add(ip.id_b);
            }
        }
        Map<Integer, String> idProdMap = new HashMap<>();
        if (useWebShop){
            idProdMap = mapIDsToSites(prods);
        }
        double[][] sims = new double[rowColIDs.size()][rowColIDs.size()];
        IntPair ip;
        for (int i = 0; i < rowColIDs.size(); i++) {
            for (int j = i; j < rowColIDs.size(); j++) {
                ip = new IntPair(rowColIDs.get(i), rowColIDs.get(j));
                if(i==j)
                    //TODO Figure out if 1 is the best value to use here for the similarity of a product with itself.
                    sims[i][j] = 1.0;
                else if (useWebShop && idProdMap.get(rowColIDs.get(i)).equals(idProdMap.get(rowColIDs.get(j)))){
                    //TODO Figure out if 0 is the best value to use here for the similarity between 2 products from the same web shop.
                    sims[i][j] = 0.0;
                    sims[j][i] = 0.0;
                } else {
                    sims[i][j] = similarities.get(ip);
                    sims[j][i] = similarities.get(ip);
                }
            }

        }
        Matrix simMat = new ArrayMatrix(sims);
        Integer[] rcids = rowColIDs.toArray(new Integer[]{});
        return new SimMatrix(simMat, rcids);
    }

    Map<Integer, ProductDesc> mapIDsToProds(Set<ProductDesc> prods){

        Map<Integer, ProductDesc> idProd = new HashMap<>();
        for(ProductDesc p : prods){
            idProd.put(p.ID,p);
        }
        return idProd;
    }

    Map<Integer, String> mapIDsToSites(Set<ProductDesc> prods){

        Map<Integer, String> idProd = new HashMap<>();
        for(ProductDesc p : prods){
            idProd.put(p.ID,p.shop);
        }
        return idProd;
    }

    public static class SimMatrix{

        private Matrix simMat;
        private Integer[] rowColIDs;

        public SimMatrix(Matrix simmat, Integer[] rcids){
            this.simMat = simmat;
            this.rowColIDs = rcids;
        }

        public Matrix getSimMat() {
            return simMat;
        }

        public Integer[] getRowColIDs() {
            return rowColIDs;
        }
    }
}
