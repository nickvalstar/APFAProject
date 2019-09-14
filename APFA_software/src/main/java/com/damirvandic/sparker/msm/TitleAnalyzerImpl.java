package com.damirvandic.sparker.msm;

import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.util.IntPair;
import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TitleAnalyzerImpl implements TitleAnalyzer {
    private static final Pattern MW_PATTERN = getMWPattern();
    private static final byte YES = 1;
    private static final byte NO = 0;
    private final Set<String> brandNamesSet;
    private final double dAlpha;
    private final double dEpsilon;
    private final Levenshtein levenshteinDistance;

    public TitleAnalyzerImpl(Set<String> brandNamesSet,
                             double dAlpha,
                             double dEpsilon) {
        this.brandNamesSet = brandNamesSet;
        this.dAlpha = dAlpha;
        this.dEpsilon = dEpsilon; //NOTE: dEpsilon is Beta in our algorithm and that of Marnix!!
        levenshteinDistance = new Levenshtein();
    }

    public TitleAnalyzerImpl(double dAlpha,
                             double dEpsilon) {
        this.brandNamesSet = new HashSet<>(Arrays.asList(new String[]{"samsung", "philips", "sony", "sharp", "nec", "hp", "toshiba", "hisense", "sony", "lg", "sanyo", "coby", "panasonic", "sansui", "vizio", "viewsonic", "sunbritetv", "haier", "optoma", "proscan", "jvc", "pyle", "sceptre", "magnavox", "mitsubishi", "supersonic", "compaq", "hannspree", "upstar", "seiki", "rca", "craig", "affinity", "naxa", "westinghouse", "epson", "elo", "sigmac"}));
        this.dAlpha = dAlpha;
        this.dEpsilon = dEpsilon; //NOTE: dEpsilon is Beta in our algorithm and that of Marnix!!
        levenshteinDistance = new Levenshtein();
    }

    private static Pattern getMWPattern() {
        String regex = "([a-zA-Z0-9]*(([0-9]+[^0-9^,^ ]+)|([^0-9^,^ ]+[0-9]+))[a-zA-Z0-9]*)";
        Pattern p = Pattern.compile(regex);
        return p;
    }

    @Override
    public boolean differentTitleBrands(ProductDesc a, ProductDesc b) {
//        IntPair p = new IntPair(a.ID, b.ID);
        return differentTitleBrands(a.title, b.title);
    }

    @Override
    public double computeSim(ProductDesc prodI, ProductDesc prodJ) {
        IntPair p = new IntPair(prodI.ID, prodJ.ID);
        double ret;
        double titleCosine = titleCosine(prodI.title, prodJ.title);

        if (titleCosine > dAlpha) {
            ret = 1.0;                            // entities are duplicates
        } else {
            double finalNameSim = titleLV(prodI, prodJ, titleCosine);
            if (finalNameSim == -1) {    //if this is true, approxSame was false
                return -1;
            } else {    //approxSame was true
                if (finalNameSim > dEpsilon) {
                    return finalNameSim; //entities are duplicates
                } else {
                    return -1;            //entities are not duplicates
                }
            }
        }
        return ret;
    }

    private double titleLV(ProductDesc prodI, ProductDesc prodJ, double titleCosine) {
        String[] modelWordsA = extractModelWords(preProcess(prodI.title));
        String[] modelWordsB = extractModelWords(preProcess(prodJ.title));
        if (approxSame(modelWordsA, modelWordsB)) {
            double finalNameSim = (0.5 * titleCosine) + ((1 - 0.5) * avgLvSim(modelWordsA, modelWordsB));
            finalNameSim = 0.5 * avgLvSimMW(modelWordsA, modelWordsB) + ((1 - 0.5) * finalNameSim);
            return finalNameSim;
        } else {
            return -1;
        }
    }

    private double titleCosine(String titleA, String titleB) {
        double ret;
        titleA = preProcess(titleA);
        titleB = preProcess(titleB);

        String[] wordsA = titleA.toLowerCase(Locale.ENGLISH).split("\\s+");
        String[] wordsB = titleB.toLowerCase(Locale.ENGLISH).split("\\s+");

        Map<String, int[]> wordCounts = new HashMap<>();
        for (String word : wordsA) {
            if (wordCounts.containsKey(word)) {
                wordCounts.get(word)[0]++;        // note that values in hashMap are stored by reference!
            } else {
                int[] counts = {1, 0};
                wordCounts.put(word, counts);
            }
        }
        for (String word : wordsB) {
            if (wordCounts.containsKey(word)) {
                wordCounts.get(word)[1]++;        // note that values in hashMap are stored by reference!
            } else {
                int[] counts = {0, 1};
                wordCounts.put(word, counts);
            }
        }

        // Compute cosine similarity
        int dotProd = 0, dotA = 0, dotB = 0;
        for (int[] counts : wordCounts.values()) {
            dotProd += counts[0] * counts[1];
            dotA += counts[0] * counts[0];
            dotB += counts[1] * counts[1];
        }
        ret = dotProd / (Math.sqrt(dotA * dotB));

        return ret;
    }

    private boolean differentTitleBrands(String a, String b) {
        boolean areDifferent = false;
        Set<String> tokensI = new HashSet<>(Arrays.asList(a.toLowerCase().split("\\s+")));
        tokensI.retainAll(brandNamesSet);    //retainAll takes intersection of (by definition unique) items of both sets

        if (!tokensI.isEmpty()) {
            Set<String> tokensJ = new HashSet<>(Arrays.asList(b.toLowerCase().split("\\s+")));
            tokensJ.retainAll(brandNamesSet);

            if (!tokensJ.isEmpty()) {
                tokensI.retainAll(tokensJ);    //tokensI now remains with the intersection of both the brandnames
                areDifferent = tokensI.isEmpty(); // if no overlapping brands remain, must be different brands
            }
        }
        return areDifferent;
    }


    private String preProcess(String str) {
        str = str.replaceAll("\\, |\\. |\\.$|\\(|\\)", " ");    //remove comma and period at end of word (or period at end of whole string)
        return str.toLowerCase(Locale.ENGLISH);
    }

    private String[] extractModelWords(String title) {
        Matcher m = MW_PATTERN.matcher(title);
        HashSet<String> c = new HashSet<>();
        while (m.find()) {
            String s = m.group(1);
            c.add(s);
        }
        String[] D = c.toArray(new String[c.size()]);
        return D;
    }

    private boolean approxSame(String[] modelWordsP, String[] modelWordsQ) {
        for (String p : modelWordsP) {
            for (String q : modelWordsQ) {
                String pNumbers = p.replaceAll("[^0-9]", "");
                String qNumbers = q.replaceAll("[^0-9]", "");

                if (!pNumbers.equals(qNumbers)) {
                    //System.out.println(pNumbers);
                    //System.out.println(qNumbers);
                    String pWords = p.replaceAll("[0-9]", "");
                    String qWords = q.replaceAll("[0-9]", "");
                    if (LevenshteinDistance.computeLevenshteinDistance(pWords, qWords) < 0.5)
                        return false;
                }
            }
        }
        return true;
    }

    private double avgLvSim(String[] modelWordsP, String[] modelWordsQ) {
        double dRet = 0;
        double dSumSumLength = 0;
        for (String p : modelWordsP) {
            for (String q : modelWordsQ) {
                double dSumLength = (double) (p.length() + q.length());
                dRet += (1 - LevenshteinDistance.computeLevenshteinDistance(p, q)) * dSumLength;
                dSumSumLength += dSumLength;
            }
        }
        dRet = dRet / dSumSumLength;
        return dRet;
    }

    private double avgLvSimMW(String[] modelWordsP, String[] modelWordsQ) {
        double dRet = 0;
        double dSumSumLength = 0;
        for (String p : modelWordsP) {
            for (String q : modelWordsQ) {
                String pNumbers = p.replaceAll("[^a-zA-Z]", "");
                String qNumbers = q.replaceAll("[^a-zA-Z]", "");
                if (pNumbers.equals(qNumbers)) {
                    String pWords = p.replaceAll("[^0-9]", "");
                    String qWords = q.replaceAll("[^0-9]", "");
                    if (LevenshteinDistance.computeLevenshteinDistance(pWords, qWords) < 0.5) {
                        double dSumLength = (double) (p.length() + q.length());
                        dRet += (1 - LevenshteinDistance.computeLevenshteinDistance(p, q)) * dSumLength;
                        dSumSumLength += dSumLength;
                    }
                }
            }
        }
        dRet = dRet / dSumSumLength;
        return dRet;
    }

}
