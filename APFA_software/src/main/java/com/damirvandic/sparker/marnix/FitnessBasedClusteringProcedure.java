package com.damirvandic.sparker.marnix;

import ch.usi.inf.sape.hac.ClusteringBuilder;
import com.damirvandic.sparker.core.ClusteringProcedure;
import com.damirvandic.sparker.core.Clusters;
import com.damirvandic.sparker.core.ClustersBuilder;
import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.msm.TitleAnalyzerImpl;
import com.damirvandic.sparker.util.IntPair;
import gnu.trove.map.TIntObjectMap;

import java.util.*;

public class FitnessBasedClusteringProcedure implements ClusteringProcedure {
    @Override
    public Clusters createClusters(Map<String, Object> conf, Map<IntPair, Double> similarities, TIntObjectMap<ProductDesc> productIDIndexMap) {
        Set<ProductDesc> products = new HashSet<>(productIDIndexMap.valueCollection());

        double fitnessAlpha = (double) conf.get("fitnessAlpha");
        double edgeThreshold = (double) conf.get("edgeThreshold");
        String method = (String) conf.get("method");
        String fitnessFunction = (String) conf.get("fitnessFunction");
        if (!fitnessFunction.equals("lanc") && !fitnessFunction.equals("addConstant") && !fitnessFunction.equals("maxSim") && !fitnessFunction.equals("maxSimACS")){
            fitnessFunction = "lanc";
            System.out.println("Fitness function name not recognized, using default method (lanc) instead.");
        }
        //This variable determines which method is used: just the number of edges or weighted edges with weights above threshold.
        //Method = uwn: unweighted edges, no overlap.
        //Method = wtn: weighted edges above threshold, no overlap.
        //Method = uwo: unweighted edges, overlap (communities are only found for unclustered products, rather than all of them).
        //Method = wto: weighted edges, overlap (communities are only found for unclustered products, rather than all of them).
        if (!method.equals("uwn") && !method.equals("wtn") && !method.equals("uwo") && !method.equals("wto")){
            method = "wtn";
            System.out.println("Method name not recognized, using default method (weighted edges, no overlap) instead.");
        }
//        double msmAlpha = (double) conf.get("alpha");
//        double msmBeta = (double) conf.get("beta");
//        boolean useBrands = (boolean) conf.get("useBrands");
//        TitleAnalyzerImpl tai = new TitleAnalyzerImpl(msmAlpha, msmBeta);


        ClustersBuilder clustersBuilder = new ClustersBuilder();
        Set<Set<ProductDesc>> clusterSets = new HashSet<>();

        Map<Integer, Set<ProductDesc>> index = new HashMap<>();
        Map<ProductDesc, Set<Integer>> prodsWithClusters = new HashMap<>();



        ArrayList<Map<Integer,Double>> edgeMaps = makeEdgeMaps(similarities, edgeThreshold);
//        Map<Integer,Double> weightedEdges = edgeMaps.get(0);
        Map<Integer,Double> maxSim = edgeMaps.get(0);
        Map<Integer,Double> weightedEdgesAboveThreshold = edgeMaps.get(1);
        Map<Integer,Double> numberOfEdges = edgeMaps.get(2);


        Map<String, Set<ProductDesc>> descriptions = mapSitesToProds(products);
//        Map<String, Integer> urlIdMap = makeUrlIdMap(products);


        Set<String> webSites = descriptions.keySet();
        String[] websites = webSites.toArray(new String[]{});
        int numberOfWebsites = webSites.size();
        Map<String, Set<ProductDesc>> unclustered = new HashMap<>();
        unclustered.putAll(descriptions);

        Random rnd = new Random(9042);
//    	int numberOfUnclusteredProducts = getNumberOfUnclusteredProducts(unclustered, webSites);
        Set<String> sitesWithUnclusteredProds = new HashSet<>();
        sitesWithUnclusteredProds.addAll(webSites);

        while (!sitesWithUnclusteredProds.isEmpty()){
            //Pick a random unclustered product and remove it from the Map containing the unclustered products.
            String[] swup = sitesWithUnclusteredProds.toArray(new String[]{});
            String currentWebsite = swup[rnd.nextInt(sitesWithUnclusteredProds.size())];
            Set<ProductDesc> currentWebsiteProducts = unclustered.get(currentWebsite);
            ProductDesc[] currentWebsiteProds = currentWebsiteProducts.toArray(new ProductDesc[]{});
            ProductDesc currentProduct = currentWebsiteProds[rnd.nextInt(currentWebsiteProds.length)];
            currentWebsiteProducts.remove(currentProduct);
            if (currentWebsiteProducts.isEmpty())
                sitesWithUnclusteredProds.remove(currentWebsite);
            unclustered.put(currentWebsite, currentWebsiteProducts);
            double largestFitness = 0;

            //Make a set with the web sites which are NOT in the current cluster, this must be updated whenever
            //a new product is added to/removed from the cluster.
            //TODO Make absolutely sure that this always happens.
            Set<String> websitesNotInCluster = new HashSet<>();
            for (String site : webSites){
                websitesNotInCluster.add(site);
            }
            websitesNotInCluster.remove(currentWebsite);
            Map<ProductDesc, String> currentClusterWithSites = new HashMap<>();
            currentClusterWithSites.put(currentProduct, currentWebsite);
            Set<Integer> currentClusterIDs = new HashSet<>();
            currentClusterIDs.add(currentProduct.ID);

            //When there is only 1 node in a cluster, there are no edges within the cluster, which means fitness = 0.
            //TODO Make absolutely sure that the number of edges within a cluster is always updated whenever a node is added/removed.
            double edgesWithinCluster = 0;
            double currentClusterFitness = 0;
            if (fitnessFunction.equals("maxSim") || fitnessFunction.equals("maxSimACS")) {
                if (method.equals("uwn") || method.equals("uwo"))
                    currentClusterFitness = calculateFitnessWithOutNode(currentClusterIDs, -99999, numberOfEdges, 0, 0, fitnessAlpha, fitnessFunction, maxSim);
                else{
                    currentClusterFitness = calculateFitnessWithOutNode(currentClusterIDs, -99999, weightedEdgesAboveThreshold, 0, 0, fitnessAlpha, fitnessFunction, maxSim);
                }
            }

            //largestFitnessProduct is initialized as currentProduct here just to make the code work (otherwise the part
            // which removes any nodes with negative fitness won't work). LargestFitnessProduct will either not be used
            // or changed to another value before it's used, therefore this value (currentProduct) will not be used.
            ProductDesc largestFitnessProduct = currentProduct;


            boolean communityIdentified = false;
            while (!communityIdentified){

                lFitnessData lfd = findHighestFitnessNode(currentClusterFitness, websitesNotInCluster, method, similarities, edgeThreshold, descriptions, currentClusterIDs, weightedEdgesAboveThreshold,
                        numberOfEdges, fitnessAlpha, edgesWithinCluster, unclustered, largestFitnessProduct, fitnessFunction, maxSim/*, useBrands, tai, currentClusterWithSites*/);




                //At this point, the node with the highest fitness w.r.t. the current cluster has been determined.
                //If this highest node fitness > 0, the node is added to the cluster.
                //After this, a check is performed to see if any nodes now have a negative fitness.
                //If such nodes are found, a removal process starts, which continues until all nodes in the cluster
                //have a positive fitness value.
                currentClusterFitness = lfd.getlClusterFitness();
                largestFitnessProduct = lfd.getlFitnessProduct();
                largestFitness = lfd.getlNodeFitness();
                if (largestFitness > 0){


                    //Add the (previously determined) node with the highest fitness to the current cluster.
                    String largestFitnessWebsite = lfd.getlFitnessWebsite();
                    websitesNotInCluster.remove(largestFitnessWebsite);
                    edgesWithinCluster += edgesToNode(currentClusterIDs, largestFitnessProduct.ID, similarities, method, edgeThreshold);
                    currentClusterWithSites.put(largestFitnessProduct, largestFitnessWebsite);
                    currentClusterIDs.add(largestFitnessProduct.ID);

                    //For methods without overlap, the newly added product is always removed from the Map of unclustered products,
                    //for methods with overlap, if the product was unclustered, it is removed from that Map, otherwise the following code does nothing
                    //(since the .remove operation just does nothing if the product wasn't in the Map anyway).
                    currentWebsiteProducts = unclustered.get(largestFitnessWebsite);
                    currentWebsiteProducts.remove(largestFitnessProduct);
                    if (currentWebsiteProducts.isEmpty())
                        sitesWithUnclusteredProds.remove(largestFitnessWebsite);
                    unclustered.put(largestFitnessWebsite, currentWebsiteProducts);



                    boolean negativeFitness = true;
                    //The last node in a cluster should never have a negative fitness.
                    while (negativeFitness && currentClusterWithSites.size()>1){
                        double lowestFitness;
                        //Note that the following 3 variables only have values here for the purpose of initialization, these values have no meaning,
                        //they are changed before these variables are ever used (if they are even used at all; (they are only used in cases where a
                        //node with negative fitness is found)).
                        double smallerClusterFitness;
                        ProductDesc lowestFitnessProduct = currentProduct;
                        String lowestFitnessWebsite;
                        //Find the node with the lowest fitness within the cluster.
                        lFitnessData lowestFitnessData = findLowestFitnessNode(currentClusterWithSites, currentClusterIDs, similarities, method, edgeThreshold,
                                weightedEdgesAboveThreshold, numberOfEdges, edgesWithinCluster, fitnessAlpha, currentClusterFitness, currentProduct, fitnessFunction, maxSim);

                        lowestFitness = lowestFitnessData.getlNodeFitness();
                        lowestFitnessWebsite = lowestFitnessData.getlFitnessWebsite();
                        lowestFitnessProduct = lowestFitnessData.getlFitnessProduct();
                        smallerClusterFitness = lowestFitnessData.getlClusterFitness();

                        if (lowestFitness >= 0){
                            negativeFitness = false;
                        } else {
                            //Remove the node with the lowest fitness from the cluster.
                            websitesNotInCluster.add(lowestFitnessWebsite);
                            currentClusterWithSites.remove(lowestFitnessProduct);
                            currentClusterIDs.remove(lowestFitnessProduct.ID);
                            edgesWithinCluster -= edgesToNode(currentClusterIDs, lowestFitnessProduct.ID, similarities, method, edgeThreshold);

                            currentClusterFitness = smallerClusterFitness;

                            //TODO Figure out a way to make this work for methods 2 and 3. Due to the possibility of overlap, if a product is removed from a cluster,
                            //it might still be in another cluster, which means it should not be put back in unclustered. A workaround would be to skip this part for those methods.
                            //Another option would be to keep track of which cluster(s) each product is in, which could come in handy when removing the overlap. That could be done whenever a cluster is finished.
                            currentWebsiteProducts = unclustered.get(lowestFitnessWebsite);
                            if (currentWebsiteProducts.isEmpty())
                                sitesWithUnclusteredProds.add(lowestFitnessWebsite);
                            currentWebsiteProducts.add(lowestFitnessProduct);
                            unclustered.put(lowestFitnessWebsite, currentWebsiteProducts);
                            if (currentClusterWithSites.size()<=1)
                                negativeFitness = true;
                        }
                    }


                } else {
                    communityIdentified = true;

//                    if (method == 2 || method == 3){
//                        if (!index.entrySet().contains(currentClusterWithSites.keySet())){
//
//                        }
//                    }
                    //This if condition is here to prevent the same cluster from being made more than once when using a method with overlap.
                    if (method.equals("uwn") || method.equals("wtn") || !index.entrySet().contains(currentClusterWithSites.keySet())){
                        //Put the finished cluster in the Map of all finished clusters. Each cluster is mapped to a unique Integer.
                        Integer clusterNumber = index.size();
                        index.put(clusterNumber, currentClusterWithSites.keySet());

                        //For each product in the cluster, add the number of the new cluster to their list of clusters they belong to.
                        for (ProductDesc pd : currentClusterWithSites.keySet()) {
                            if (prodsWithClusters.containsKey(pd)) {
                                Set<Integer> clusterNumbers = prodsWithClusters.get(pd);
                                clusterNumbers.add(clusterNumber);
                                prodsWithClusters.put(pd, clusterNumbers);
                            } else {
                                Set<Integer> clusterNumbers = new HashSet<>();
                                clusterNumbers.add(clusterNumber);
                                prodsWithClusters.put(pd, clusterNumbers);
                            }
                        }
                    }
                }


            }
        }



        //For the methods involving overlap, the following code is used to remove any overlap.
        if (method.equals("wto") || method.equals("uwo")){
            //Loop over all (now clustered) products.
            for (ProductDesc prod : prodsWithClusters.keySet()){
                Set<Integer> clusterNumbers = prodsWithClusters.get(prod);
                //If a product is in more than 1 cluster, there is overlap; continue into the loop to handle the overlap.
                if (clusterNumbers.size()>1){
                    Integer[] cns = clusterNumbers.toArray(new Integer[]{});
//    				//Loop over all the clusters this product is in, compa
//    				for (int i=0; i<cns.length-1; i++){
//    					Set<ProductDescription> cluster0 = index.get(cns[i]);
//    					Set<ProductDescription> cluster1 = index.get(cns[i+1]);
//    					//If the size of either cluster is 1, that means the overlap can only be the one product in this cluster.
//    					//
//    					if (cluster0.size()>2 && cluster1.size()>2){
//
//    					} else {
//
//    					}
//    				}
                    ArrayList<Set<ProductDesc>> olClusters = new ArrayList<Set<ProductDesc>>();
                    ArrayList<Set<Integer>> olClustersIDs = new ArrayList<Set<Integer>>();

                    //Make ArrayLists of Sets containing all clusters that the current
                    //product is in; 1 with the urls and 1 with the full ProductDescriptions
                    for (int i=0; i<cns.length; i++){
                        olClusters.add(i, index.get(cns[i]));
                        Set<Integer> olIDs = new HashSet<>();
                        for (ProductDesc cProd : olClusters.get(i)){
                            olIDs.add(cProd.ID);
                        }
                        olClustersIDs.add(i, olIDs);
                    }
                    double fitNoNode;
                    double fitWithNode;
                    double nodeEdges;
                    double edgesInCluster;
                    double fitDiff;
                    double maxFitDiff=-999999;
                    int maxFitDiffCn = 0;
                    //Loop over all clusters containing the current Product. Calculate the fitness with and without the current product in the cluster.
                    //Calculate the difference between those 2 values for each cluster. The cluster for which this value is the largest, retains the product,
                    //it is removed from the other clusters (the max of this value means that the overall fitness decreases the most if the product was
                    //removed from this cluster).
                    for (int cn=0; cn<olClusters.size(); cn++){
                        nodeEdges = edgesToNode(olClustersIDs.get(cn), prod.ID, similarities, method, edgeThreshold);
                        edgesInCluster = edgesWithinCluster(olClusters.get(cn), similarities, method, edgeThreshold);
                        if (method.equals("uwo")) {
                            fitNoNode = calculateFitnessWithOutNode(olClustersIDs.get(cn), prod.ID, numberOfEdges, nodeEdges, edgesInCluster, fitnessAlpha, fitnessFunction, maxSim);
                            //calculateFitnessWithOutNode is used for the fitness WITH the node as well, since it is also used to calculate the fitness of a
                            //cluster without removing anything (calculateFitnessWithNode is only used to calculate the fitness when adding a node to a cluster.
                            //The -99999 is just a random number that should not be equal to any product ID (which should be the case, assuming that all ID's are positive).
                            fitWithNode = calculateFitnessWithOutNode(olClustersIDs.get(cn), -99999, numberOfEdges, 0, edgesInCluster, fitnessAlpha, fitnessFunction, maxSim);
                        } else {
                            fitNoNode = calculateFitnessWithOutNode(olClustersIDs.get(cn), prod.ID, weightedEdgesAboveThreshold, nodeEdges, edgesInCluster, fitnessAlpha, fitnessFunction, maxSim);
                            fitWithNode = calculateFitnessWithOutNode(olClustersIDs.get(cn), -99999, weightedEdgesAboveThreshold, 0, edgesInCluster, fitnessAlpha, fitnessFunction, maxSim);
                        }
                        fitDiff = fitWithNode - fitNoNode;
                        if (fitDiff>maxFitDiff){
                            maxFitDiff = fitDiff;
                            maxFitDiffCn = cn;
                        }
                    }

                    //Remove the current product from all but one cluster. If this results in empty clusters: remove those from index.
                    Set<ProductDesc> tempCluster = new HashSet<>();
                    for (int cn=0; cn<olClusters.size(); cn++){
                        if (cn != maxFitDiffCn){
                            tempCluster = olClusters.get(cn);
                            tempCluster.remove(prod);
                            if (tempCluster.isEmpty()){
                                index.remove(cns[cn]);
                            } else {
                                index.put(cns[cn], tempCluster);
                            }
                        }
                    }


                }


            }
        }

        //Create a map of all model ID's to the numbers of the clusters they're in. If an ID is in multiple clusters (i.e.
        //the size of the Set is larger than 1), then it has been incorrectly clustered.
        Map<String, Set<Integer>> modelIDClusters = new HashMap<>();
        for (Integer clusterNum : index.keySet()){
            Set<ProductDesc> currentCluster = index.get(clusterNum);
            for (ProductDesc pDesc : currentCluster){
                if (!modelIDClusters.containsKey(pDesc.modelID)){
                    Set<Integer> cn = new HashSet<>();
                    cn.add(clusterNum);
                    modelIDClusters.put(pDesc.modelID, cn);
                } else{
                    Set<Integer> cn = modelIDClusters.get(pDesc.modelID);
                    if (!cn.contains(clusterNum)) {
                        cn.add(clusterNum);
                        modelIDClusters.put(pDesc.modelID, cn);
                    }
                }

            }
        }


        //The following codes separates clusters in correct clusters, too small clusters (which have only duplicates of 1 product in them, but not all
        //of the duplicates of that product) and otherwise wrong clusters (clusters which have non-duplicate products in them).  At the moment this is
        // not output, but only looked at in the debugger.
        Map<Integer, Set<ProductDesc>> tooSmallClusters = new HashMap<>();
        Map<Integer, Set<ProductDesc>> otherwiseWrongClusters = new HashMap<>();
        Map<Integer, Set<ProductDesc>> correctClusters = new HashMap<>();
        Map<Integer, Set<ProductDesc>> correctSingletonClusters = new HashMap<>();
        Map<Integer, Set<ProductDesc>> correctLargerClusters = new HashMap<>();
        for (Integer clusterNum : index.keySet()){
            Set<ProductDesc> currentCluster = index.get(clusterNum);
            boolean multipleModelIDsInCluster = false;
            ProductDesc[] prodsArray = currentCluster.toArray(new ProductDesc[]{});
            String mID = prodsArray[0].modelID;
            if(currentCluster.size()>1) {
                int i = 1;
                while (!multipleModelIDsInCluster && i < prodsArray.length){
                    if (!mID.equals(prodsArray[i].modelID))
                        multipleModelIDsInCluster = true;
                    i++;
                }
            }
            if (multipleModelIDsInCluster){
                otherwiseWrongClusters.put(clusterNum, currentCluster);
            }else if(modelIDClusters.get(mID).size()>1){
                tooSmallClusters.put(clusterNum, currentCluster);
            }else {
                correctClusters.put(clusterNum, currentCluster);
                if(currentCluster.size()==1)
                    correctSingletonClusters.put(clusterNum, currentCluster);
                else
                    correctLargerClusters.put(clusterNum, currentCluster);
            }
        }


        clusterSets = new HashSet<>(index.values());


        return clustersBuilder.fromSets(clusterSets);
    }

    @Override
    public String componentID() {
        return "fbcp"; // FitnessBasedClusteringProcedure
    }


    Map<String, Set<ProductDesc>> mapSitesToProds(Set<ProductDesc> prods){

        Map<String, Set<ProductDesc>> desc = new HashMap<>();
        for(ProductDesc p : prods){
            String site = p.shop;
            if (desc.keySet().contains(site)){
                Set<ProductDesc> siteProds = desc.get(site);
                siteProds.add(p);
                desc.put(site,siteProds);
            }else{
                Set<ProductDesc> prodSet = new HashSet<>();
                prodSet.add(p);
                desc.put(site,prodSet);
            }
        }
        return desc;
    }


    /**
     * @param ids  The ID(s) of the product(s) in the cluster.
     * @param nodeID  The ID of the node.
     * @param sims
     * @param m
     * @param threshold
     * @return  The number of edges from the node to the cluster.
     */
    public double edgesToNode(Set<Integer> ids, int nodeID, Map<IntPair, Double> sims /*, Map<String, Integer> urlIdMap*/, String m, double threshold){
        //The following line removes the node url from the cluster if it is in the cluster, and
        //does nothing otherwise. This is done so that this method can determine the number of
        //edges from a node (which is not in the cluster) to the cluster as well as determine the
        //number of edges from a node within a cluster to the other nodes in the cluster.
        boolean nodeRemoved = false;
        if (ids.contains(nodeID)) {
            ids.remove(nodeID);
            nodeRemoved = true;
        }

        IntPair currentProds;
        double edges = 0;
//        String[] urlArray = urls.toArray(new String[]{});
//        Set<Integer> ids = new HashSet<>();
//        for (String url : urls) {
//            ids.add(urlIdMap.get(url));
//        }
        Integer[] idArray = ids.toArray(new Integer[]{});
//        currentProds = new IntPair(nodeID,idArray[0]);
        for (int i=0; i<ids.size(); i++){
            currentProds = new IntPair(nodeID,idArray[i]);
            if (m.equals("wtn") || m.equals("wto")){
                if (sims.get(currentProds)>=threshold)
                    edges += sims.get(currentProds);
            } else if (m.equals("uwn") || m.equals("uwo")){
                if (sims.get(currentProds)>=threshold)
                    edges++;
//            } else{
//                edges += sims.get(currentProds);
            }
            i++;
//            if (i<ids.size())
//                currentProds = new IntPair(idArray[i-1],idArray[i]);
        }
        //TODO I feel like the .remove shouldn't have any influence on urls beyond this method; apparently it does, so the next line re-adds it.
        if (nodeRemoved)
            ids.add(nodeID);
        return edges;
    }


    public ArrayList<Map<Integer,Double>> makeEdgeMaps(Map<IntPair, Double> similarities, double edgeThreshold){

//        Map<Integer,Double> weightedEdges = new HashMap<>();
        Map<Integer,Double> weightedEdgesAboveThreshold = new HashMap<>();
        Map<Integer,Double> numberOfEdges = new HashMap<>();
        Map<Integer,Double> maxSim = new HashMap<>();

        //This for loop fills up the Maps NumberOfEdges, weightedEdges and weightedEdgesAboveThreshold.
        for (IntPair ip : similarities.keySet()){
            if (similarities.get(ip)>=edgeThreshold){
                if(numberOfEdges.containsKey(ip.id_a)){
                    //This if statement is used for weightedEdgesAboveThreshold and numberOfEdges at the same time, this works because both Maps always contain the same keys.
                    Double nwEdges = numberOfEdges.get(ip.id_a);
                    numberOfEdges.put(ip.id_a, nwEdges+1);
                    nwEdges = weightedEdgesAboveThreshold.get(ip.id_a);
                    weightedEdgesAboveThreshold.put(ip.id_a, nwEdges+similarities.get(ip));
                    if (similarities.get(ip)>maxSim.get(ip.id_a)) {
                        maxSim.put(ip.id_a, similarities.get(ip));
                    }
                }else{
                    numberOfEdges.put(ip.id_a, 1.0);
                    weightedEdgesAboveThreshold.put(ip.id_a, similarities.get(ip));
                    maxSim.put(ip.id_a,similarities.get(ip));
                }
                if(numberOfEdges.containsKey(ip.id_b)){
                    Double nwEdges = numberOfEdges.get(ip.id_b);
                    numberOfEdges.put(ip.id_b, nwEdges+1);
                    nwEdges = weightedEdgesAboveThreshold.get(ip.id_b);
                    weightedEdgesAboveThreshold.put(ip.id_b, nwEdges+similarities.get(ip));
                    if (similarities.get(ip)>maxSim.get(ip.id_b)) {
                        maxSim.put(ip.id_b, similarities.get(ip));
                    }
                }else{
                    numberOfEdges.put(ip.id_b, 1.0);
                    weightedEdgesAboveThreshold.put(ip.id_b, similarities.get(ip));
                    maxSim.put(ip.id_b,similarities.get(ip));
                }
            }else{
                if(!numberOfEdges.containsKey(ip.id_a)){
                    numberOfEdges.put(ip.id_a, 0.0);
                    weightedEdgesAboveThreshold.put(ip.id_a, 0.0);
                    maxSim.put(ip.id_a, 0.0);
                }
                if(!numberOfEdges.containsKey(ip.id_b)){
                    numberOfEdges.put(ip.id_b, 0.0);
                    weightedEdgesAboveThreshold.put(ip.id_b, 0.0);
                    maxSim.put(ip.id_b, 0.0);
                }
            }


//            if (weightedEdges.containsKey(ip.id_a)){
//                Double weight = weightedEdges.get(ip.id_a);
//                weightedEdges.put(ip.id_a, weight+similarities.get(ip));
//            } else {
//                weightedEdges.put(ip.id_a, similarities.get(ip));
//            }
//            if (weightedEdges.containsKey(ip.id_b)){
//                Double weight = weightedEdges.get(ip.id_b);
//                weightedEdges.put(ip.id_b, weight+similarities.get(ip));
//            } else {
//                weightedEdges.put(ip.id_b, similarities.get(ip));
//            }

        }

        ArrayList<Map<Integer,Double>> edgeMaps = new ArrayList<Map<Integer,Double>>();
//        edgeMaps.add(weightedEdges);
        edgeMaps.add(maxSim);
        edgeMaps.add(weightedEdgesAboveThreshold);
        edgeMaps.add(numberOfEdges);
        return edgeMaps;
    }

    public Map<String, Integer> makeUrlIdMap(Set<ProductDesc> prods){
        Map<String, Integer> urlIdMap = new HashMap<>();
        for (ProductDesc p : prods){
            urlIdMap.put(p.url, p.ID);
        }
        return urlIdMap;
    }


    /**
     * Calculate the fitness of a cluster when a node is added to it.
     * @param clusteredIDs  The IDs of the products in the cluster (not including the node to be added).
     * @param nodeID  The ID of the node to be added.
     * @param numberOfEdges  A Map that Maps product ID's to the number of edges or the sum of the weights of the edges from that node (product) to all other nodes.
     * @param numberOfEdgesToNode  The number of edges or the sum of the weights of the edges from the node to be removed to the other nodes within the cluster (must be set to 0 when no nodes are removed).
     * @param edgesWithinCluster  The total number of edges within the cluster (NOT including the number of edges to/from the node to be added).
     * @param fitnessAlpha
     * @return  The fitness of a cluster after adding a node.
     */
    public double calculateFitnessWithNode(Set<Integer> clusteredIDs, Integer nodeID, Map<Integer,Double> numberOfEdges, double numberOfEdgesToNode, double edgesWithinCluster, double fitnessAlpha, String fitnessFunc, Map<Integer, Double> maxSim){
        double totalEdgesFromCluster = 0;
        for (Integer id : clusteredIDs){
            totalEdgesFromCluster += numberOfEdges.get(id);
        }
        double kgin = edgesWithinCluster + numberOfEdgesToNode;
        double kgout = totalEdgesFromCluster - (2 * edgesWithinCluster) + numberOfEdges.get(nodeID) - (2 * numberOfEdgesToNode);


//        if (fitnessFunc.equals("maxSim") && clusteredIDs.size()==1){
//            double ms=0;
//            for (Integer id : clusteredIDs){
//                ms = maxSim.get(id);
//            }
//            return (kgin + (1-ms))/ Math.pow(kgin + kgout, fitnessAlpha);

        /*} else */if (fitnessFunc.equals("addConstant")){
            return (1+kgin) / Math.pow(kgin + kgout, fitnessAlpha);

        }else if (fitnessFunc.equals("maxSimACS")) {
            double ms = 0;
            for (Integer id : clusteredIDs) {
                if (maxSim.get(id)>ms)
                    ms = maxSim.get(id);
            }
            return (kgin + (1 - ms)) / Math.pow(kgin + kgout, fitnessAlpha);

        }else{
            return kgin / Math.pow(kgin + kgout, fitnessAlpha);
        }
    }


    /**
     * Calculate the fitness of a cluster in general or after removing a node.
     * @param clusteredIDs  The IDs of the products in the cluster (including the node to be removed (if any)).
     * @param nodeID  The ID of the node to be removed; if this method is used to calculate the fitness of a cluster in general (without removing any nodes), then this parameter can be any Integer which is NOT equal to any ID within the cluster.
     * @param numberOfEdges  A Map that Maps product ID's to the number of edges or the sum of the weights of the edges from that node (product) to all other nodes.
     * @param numberOfEdgesToNode  The number of edges or the sum of the weights of the edges from the node to be removed to the other nodes within the cluster (must be set to 0 when no nodes are removed).
     * @param edgesWithinCluster  The total number of edges within the cluster (including the edges to the node to be removed).
     * @param fitnessAlpha
     * @return  The fitness of a cluster in general or after removing a node.
     */
    public double calculateFitnessWithOutNode(Set<Integer> clusteredIDs, Integer nodeID, Map<Integer,Double> numberOfEdges, double numberOfEdgesToNode, double edgesWithinCluster, double fitnessAlpha, String fitnessFunc, Map<Integer, Double> maxSim){

        //This method can be used to determine the fitness of a cluster in general (as in, just a cluster rather than a cluster minus one node), by just using a non-existent node url as input.
        //This is because the .remove operation that the method starts with, only removes a node if nodeUrl is the url of a node contained in the cluster specified by clusteredUrls and does
        //nothing otherwise.
        //If this method is used to calculate the fitness of a cluster without removing any nodes, then numberOfEdgesToNode should be set to 0.
        boolean nodeRemoved = false;
        if (clusteredIDs.contains(nodeID)) {
            clusteredIDs.remove(nodeID);
            nodeRemoved = true;
        }


        if (clusteredIDs.isEmpty()){
            //Zero would probably work here as well; -1 is taken here to be just a little bit more certain that an empty cluster is never preferred over a non-empty one.
            //TODO Double-check if a negative value here doesn't cause any problems anywhere.
            return -1;
        } else {

            double totalEdgesFromCluster = 0;
            for (Integer id : clusteredIDs)
                totalEdgesFromCluster += numberOfEdges.get(id);

            double kgin = edgesWithinCluster - numberOfEdgesToNode;
            double kgout = totalEdgesFromCluster - (2 * kgin);

            if (nodeRemoved)
                clusteredIDs.add(nodeID);

            if (fitnessFunc.equals("maxSim") && (clusteredIDs.size()==1 || (clusteredIDs.size()==2 && nodeRemoved))){
                double ms=0;
                for (Integer id : clusteredIDs){
                    ms = maxSim.get(id);
                }
                return (kgin + (1-ms))/ Math.pow(kgin + kgout, fitnessAlpha);

            }else if (fitnessFunc.equals("maxSimACS")) {
                double ms = 0;
                for (Integer id : clusteredIDs) {
                    if (maxSim.get(id)>ms)
                        ms = maxSim.get(id);
                }
                return (kgin + (1 - ms)) / Math.pow(kgin + kgout, fitnessAlpha);

            }else if (fitnessFunc.equals("addConstant")){
                return (1+kgin) / Math.pow(kgin + kgout, fitnessAlpha);

            }else{
                return kgin / Math.pow(kgin + kgout, fitnessAlpha);
            }
        }
    }



    /**
     * Calculates the number of edges within a cluster.
     * @param fullCluster The cluster.
     * @param sims
     * @param m  The name of the method used.
     * @param threshold
     * @return  The number of edges within a cluster.
     */
    public double edgesWithinCluster(Set<ProductDesc> fullCluster, Map<IntPair, Double> sims, String m, double threshold){
        if (fullCluster.size()<=1)
            return 0;
        else if(fullCluster.size()==2)
            return 1;
        else{

            ArrayList<Integer> ids = new ArrayList<Integer>();
            for (ProductDesc pd : fullCluster){
                ids.add(pd.ID);
            }
            double numberOfEdges = 0;
            Set<Integer> partialCluster = new HashSet<>();
            partialCluster.add(ids.get(0));

            //This loop starts at 1 in stead of 0, since the partial cluster starts with the first element from urls already in it.
            for (int i=1; i<ids.size(); i++){
                numberOfEdges+=edgesToNode(partialCluster, ids.get(i), sims, m, threshold);
            }
            return numberOfEdges;
        }
    }

    public lFitnessData findHighestFitnessNode(double currentClusterFitness, Set<String> websitesNotInCluster, String method, Map<IntPair, Double> similarities, double edgeThreshold, Map<String, Set<ProductDesc>> descriptions,
                                               Set<Integer> currentClusterIDs, Map<Integer,Double> weightedEdgesAboveThreshold, Map<Integer,Double> numberOfEdges, double fitnessAlpha, double edgesWithinCluster,
                                               Map<String, Set<ProductDesc>> unclustered, ProductDesc largestFitnessProduct, String fitnessFunction, Map<Integer, Double> maxSim/*, boolean useBrands, TitleAnalyzerImpl tai,
                                               Map<ProductDesc, String> currentClusterWithSites*/){

        double numberOfEdgesToNode;
        String largestFitnessWebsite = "";
        double largestFitness = -1;
        double largestClusterFitness = currentClusterFitness;

        //Loop over all products not from the same website as any of the clustered products.
        for (String site : websitesNotInCluster){
            double fitnessWithNode;
            double nodeFitness;

            if (method.equals("uwo") || method.equals("wto") /*|| method.equals("awo")*/){

                //Note the difference with the methods without overlap here: to make overlap possible, these methods look at all products (from sites which aren't in the cluster yet),
                //rather than only the unclustered ones.
                for (ProductDesc p : descriptions.get(site)) {
//                    if (useBrands){
//                        boolean matchingTitleProdFound = false;
//                        for (ProductDesc clusteredPD : currentClusterWithSites.keySet()){
//                            if (!tai.differentTitleBrands(p, clusteredPD))
//                                matchingTitleProdFound = true;
//                        }
//                        if (matchingTitleProdFound){
//                            numberOfEdgesToNode = edgesToNode(currentClusterIDs, p.ID, similarities, method, edgeThreshold);
//                        }else{
//                            //If the number of edges to a node is 0, it is not considered as a candidate to be added to the cluster; which is what should happen when the brand name
//                            //of the product does not match with that of an already clustered product.
//                            numberOfEdgesToNode = 0;
//                        }
//
//                    }else {
                        numberOfEdgesToNode = edgesToNode(currentClusterIDs, p.ID, similarities, method, edgeThreshold);
//                    }
                    if (numberOfEdgesToNode>0){
                        if (method.equals("wto"))
                            fitnessWithNode = calculateFitnessWithNode(currentClusterIDs, p.ID, weightedEdgesAboveThreshold, numberOfEdgesToNode, edgesWithinCluster, fitnessAlpha, fitnessFunction, maxSim);
//                                else if (method.equals("awo"))
//                                    fitnessWithNode = calculateFitnessWithNode(currentClusterUrls, p.url, weightedEdges, numberOfEdgesToNode, edgesWithinCluster, fitnessAlpha, urlIdMap);
                        else
                            fitnessWithNode = calculateFitnessWithNode(currentClusterIDs, p.ID, numberOfEdges, numberOfEdgesToNode, edgesWithinCluster, fitnessAlpha, fitnessFunction, maxSim);
                        nodeFitness = fitnessWithNode - currentClusterFitness;
                        if (nodeFitness > largestFitness){
                            largestFitness = nodeFitness;
                            largestFitnessProduct = p;
                            largestFitnessWebsite = site;
                            largestClusterFitness = fitnessWithNode;
                        }
                    }
                }

            }else{
                for (ProductDesc p : unclustered.get(site)) {
//                    if (useBrands){
//                        boolean matchingTitleProdFound = false;
//                        for (ProductDesc clusteredPD : currentClusterWithSites.keySet()){
//                            if (!tai.differentTitleBrands(p, clusteredPD))
//                                matchingTitleProdFound = true;
//                        }
//                        if (matchingTitleProdFound){
//                            numberOfEdgesToNode = edgesToNode(currentClusterIDs, p.ID, similarities, method, edgeThreshold);
//                        }else{
//                            //If the number of edges to a node is 0, it is not considered as a candidate to be added to the cluster; which is what should happen when the brand name
//                            //of the product does not match with that of an already clustered product.
//                            numberOfEdgesToNode = 0;
//                        }
//
//                    }else {
                        numberOfEdgesToNode = edgesToNode(currentClusterIDs, p.ID, similarities, method, edgeThreshold);
//                    }
                    if (numberOfEdgesToNode>0){
                        if (method.equals("wtn"))
                            fitnessWithNode = calculateFitnessWithNode(currentClusterIDs, p.ID, weightedEdgesAboveThreshold, numberOfEdgesToNode, edgesWithinCluster, fitnessAlpha, fitnessFunction, maxSim);
                        else
                            fitnessWithNode = calculateFitnessWithNode(currentClusterIDs, p.ID, numberOfEdges, numberOfEdgesToNode, edgesWithinCluster, fitnessAlpha, fitnessFunction, maxSim);
                        nodeFitness = fitnessWithNode - currentClusterFitness;
                        if (nodeFitness > largestFitness){
                            largestFitness = nodeFitness;
                            largestFitnessProduct = p;
                            largestFitnessWebsite = site;
                            largestClusterFitness = fitnessWithNode;
                        }
                    }
                }
            }
        }

        return new lFitnessData(largestFitness, largestFitnessProduct, largestFitnessWebsite, largestClusterFitness);
    }

    public lFitnessData findLowestFitnessNode(Map<ProductDesc, String> currentClusterWithSites, Set<Integer> currentClusterIDs, Map<IntPair, Double> similarities, String method, double edgeThreshold, Map<Integer,Double> weightedEdgesAboveThreshold,
                                              Map<Integer,Double> numberOfEdges, double edgesWithinCluster, double fitnessAlpha, double currentClusterFitness, ProductDesc randomProductDesc, String fitnessFunction, Map<Integer, Double> maxSim){
        double lowestFitness = 0;
        ProductDesc lowestFitnessProduct = randomProductDesc;
        String lowestFitnessWebsite = "";
        double smallerClusterFitness = 0;
        for (ProductDesc productToRemove : currentClusterWithSites.keySet()){
            double numberOfEdgesToNode = edgesToNode(currentClusterIDs, productToRemove.ID, similarities, method, edgeThreshold);
            double fitnessWithoutNode;
            if (method.equals("wtn") || method.equals("wto"))
                fitnessWithoutNode = calculateFitnessWithOutNode(currentClusterIDs, productToRemove.ID, weightedEdgesAboveThreshold, numberOfEdgesToNode, edgesWithinCluster, fitnessAlpha, fitnessFunction, maxSim);
            else
                fitnessWithoutNode = calculateFitnessWithOutNode(currentClusterIDs, productToRemove.ID, numberOfEdges, numberOfEdgesToNode, edgesWithinCluster, fitnessAlpha, fitnessFunction, maxSim);
            double nodeFitness = currentClusterFitness - fitnessWithoutNode;
            if (nodeFitness < lowestFitness){
                lowestFitness = nodeFitness;
                smallerClusterFitness = fitnessWithoutNode;
                lowestFitnessProduct = productToRemove;
                lowestFitnessWebsite = currentClusterWithSites.get(lowestFitnessProduct);
            }
        }
        return new lFitnessData(lowestFitness, lowestFitnessProduct, lowestFitnessWebsite, smallerClusterFitness);
    }

    public static class lFitnessData{
        private double lNodeFitness;
        private ProductDesc lFitnessProduct;
        private String lFitnessWebsite;
        private double lClusterFitness;


        public lFitnessData(double lnf, ProductDesc lfp, String lfw, double lcf){
            this.lNodeFitness = lnf;
            this.lFitnessProduct = lfp;
            this.lFitnessWebsite = lfw;
            this.lClusterFitness = lcf;
        }

        public double getlNodeFitness() {
            return lNodeFitness;
        }

        public void setlNodeFitness(double lNodeFitness) {
            this.lNodeFitness = lNodeFitness;
        }

        public ProductDesc getlFitnessProduct() {
            return lFitnessProduct;
        }

        public void setlFitnessProduct(ProductDesc lFitnessProduct) {
            this.lFitnessProduct = lFitnessProduct;
        }

        public String getlFitnessWebsite() {
            return lFitnessWebsite;
        }

        public void setlFitnessWebsite(String lFitnessWebsite) {
            this.lFitnessWebsite = lFitnessWebsite;
        }

        public double getlClusterFitness() {
            return lClusterFitness;
        }

        public void setlClusterFitness(double lClusterFitness) {
            this.lClusterFitness = lClusterFitness;
        }
    }

}
