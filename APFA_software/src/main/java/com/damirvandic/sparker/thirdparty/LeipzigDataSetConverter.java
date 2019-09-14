package com.damirvandic.sparker.thirdparty;

import com.damirvandic.sparker.core.ConnectedComponentsFinder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class LeipzigDataSetConverter {
    private final String pathA;
    private final String shopA;
    private final String pathB;
    private final String shopB;
    private final String pathMapping;
    private Logger log;

    public LeipzigDataSetConverter(String pathA, String shopA, String pathB, String shopB, String pathMapping) {
        this.pathA = pathA;
        this.shopA = shopA;
        this.pathB = pathB;
        this.shopB = shopB;
        this.pathMapping = pathMapping;
        this.log = LoggerFactory.getLogger(getClass());
    }


    public static void main(String[] args) throws IOException {
//        String base = "./hdfs/Abt-Buy/sources/";
//        LeipzigDataSetConverter converter = new LeipzigDataSetConverter(base + "abt.csv", "abt.com", base + "buy.csv", "buy.com", base + "mapping.csv");
//        converter.writeDataSet(base + "../dataset.json");
        String base = "./hdfs/Amazon-GoogleProducts/sources/";
        LeipzigDataSetConverter converter = new LeipzigDataSetConverter(base + "amazon.csv", "amazon.com", base + "googleproducts.csv", "googleproducts.com", base + "mapping.csv");
        converter.writeDataSet(base + "../dataset.json");
    }

    public void writeDataSet(String path) throws IOException {
        Map<String, SimpleProduct> prodsA = readProducts(pathA, shopA);
        Map<String, SimpleProduct> prodsB = readProducts(pathB, shopB);

        // modelID -> [prod1,prod2, etc.]
        Multimap<String, SimpleProduct> mappings = constructMappings(prodsA, prodsB);

        Map<String, List<Map<String, Object>>> map = new HashMap<>();
        for (String modelID : mappings.keySet()) {
            Collection<SimpleProduct> products = mappings.get(modelID);
            List<Map<String, Object>> list = new ArrayList<>(products.size());
            for (SimpleProduct p : products) {
                Map<String, Object> pMap = new HashMap<>();
                pMap.put("shop", p.shop);
                pMap.put("url", String.format("http://%s/%s", p.shop, p.ID));
                pMap.put("modelID", modelID);
                pMap.put("title", p.title);
                pMap.put("featuresMap", p.featuresMap());
                list.add(pMap);
            }
            map.put(modelID, list);
        }
        write(path, map);
    }

    private void write(String path, Map<String, List<Map<String, Object>>> map) throws IOException {
        File f = new File(path);
        Preconditions.checkArgument(!(f.exists() && !f.isFile())); // can't be an existing path
        PrintWriter writer = new PrintWriter(new FileWriter(f));

        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(writer, map);
    }

    private Multimap<String, SimpleProduct> constructMappings(Map<String, SimpleProduct> prodsA, Map<String, SimpleProduct> prodsB) throws IOException {
        ImmutableMultimap.Builder<String, SimpleProduct> builder = ImmutableMultimap.builder();
        Set<Pair<SimpleProduct, SimpleProduct>> edges = new HashSet<>(prodsA.size() + prodsB.size());

        BufferedReader br = new BufferedReader(new FileReader(pathMapping));
        br.readLine(); //skip header
        String line;
        int skipped = 0;
        while ((line = br.readLine()) != null) {
            String[] split = line.split(",");
            Preconditions.checkState((split.length == 2));
            SimpleProduct a = prodsA.get(rmQuotes(split[0]));
            if (a == null) {
                log.info("cannot find product {}", a);
            }
            SimpleProduct b = prodsB.get(rmQuotes(split[1]));
            if (b == null) {
                log.info("cannot find product {}", b);
            }
            if (a != null && b != null) {
                edges.add(new ImmutablePair<>(a, b));
            }else{
                ++skipped;
            }
        }
        log.info("Skipped {} mappings", skipped);
        br.close();
        ConnectedComponentsFinder<SimpleProduct> finder = new ConnectedComponentsFinder<>(edges);
        Set<Set<SimpleProduct>> clusters = finder.findClusters();
        int i = 1;
        for (Set<SimpleProduct> cluster : clusters) {
            builder.putAll(String.valueOf(i), cluster);
            ++i;
        }
        return builder.build();
    }

    private String rmQuotes(String s) {
        if (s.startsWith("\"")) {
            s = s.substring(1);
        }
        if (s.endsWith("\"")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }

    private Map<String, SimpleProduct> readProducts(String path, String shop) throws IOException {
        ImmutableMap.Builder<String, SimpleProduct> builder = ImmutableMap.builder();
        BufferedReader br = new BufferedReader(new FileReader(path));
        List<String> header = Arrays.asList(br.readLine().replaceAll("\"", "").split(","));
        Preconditions.checkState(header.get(0).equals("id"));
        Preconditions.checkState((header.get(1).equals("title")));
        Preconditions.checkState((header.get(2).equals("description")));
        boolean brandPresent = header.size() > 3 && header.get(3).equals("manufacturer");
        String line;
        int skipped = 0;
        int processed = 0;
        while ((line = br.readLine()) != null) {
            String[] split = line.split(",");
//            Preconditions.checkState((header.size() == split.length));
            if (split.length >= 3) {
                String id = rmQuotes(split[0]);
                String title = rmQuotes(split[1]);
                String desc = rmQuotes(split[2]);
                String brand = rmQuotes(brandPresent ? split[3] : "");
                builder.put(id, new SimpleProduct(id, title, desc, shop, brand));
                ++processed;
            } else {
                ++skipped;
            }
        }
        br.close();
        log.info("Processed {} products", processed);
        log.info("Skipped {} products", skipped);
        return builder.build();
    }


    static class SimpleProduct {
        private final String ID;
        private final String title;
        private final String desc;
        private final String shop;
        private final String brand;

        SimpleProduct(String ID, String title, String desc, String shop, String brand) {
            this.ID = ID;
            this.title = title;
            this.desc = desc;
            this.shop = shop;
            this.brand = brand;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SimpleProduct that = (SimpleProduct) o;

            if (!ID.equals(that.ID)) return false;
            if (!desc.equals(that.desc)) return false;
            if (!shop.equals(that.shop)) return false;
            if (!brand.equals(that.brand)) return false;
            if (!title.equals(that.title)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = ID.hashCode();
            result = 31 * result + title.hashCode();
            result = 31 * result + desc.hashCode();
            result = 31 * result + shop.hashCode();
            result = 31 * result + brand.hashCode();
            return result;
        }

        public Map<String, String> featuresMap() {
            if (StringUtils.isNotEmpty(brand)) {
                return ImmutableMap.of("description", desc, "brand", brand);
            } else {
                return ImmutableMap.of("description", desc);
            }
        }
    }
}
