package com.damirvandic.sparker.students.group6.UniformProductDescription;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wkuipers on 11-10-14.
 */
public class TypeClassifier {
    private KeyValues keyValues;
    private double beta;  // for type classification try around 0.9
    private HashMap<String, String> dataTypes;

    // Regex definitions
    private String numReg = "(([0-9],*)+(?:(\\.\\d+)|(-\\d+/\\d+))?)";
    private String firstNumReg = "^" + numReg;

    // DataType descriptions
    private String bool = "b";
    private String text = "t";
    private String universal = "u";
    private String[] numberDim = new String[]{"", "n1", "n2", "n3", "n4"};
    private String[] numberDimUnit = new String[]{"", "n1u", "n2u", "n3u, n4u"};

    // Compounder symbols
    private ArrayList<String> compounders = new ArrayList<>();


    public TypeClassifier(KeyValues kvs, double b) {
        keyValues = kvs;
        beta = b;

        dataTypes = new HashMap<>();

        for (String key : keyValues.getKeys()) {
            dataTypes.put(key, classify(key));
        }
        compounders.add(":");
        compounders.add("x");

    }

    public String getDataType(String key) {
        return dataTypes.get(key);
    }

    public double getBeta() {
        return beta;
    }

    // classify according to classification tree, return dataType from {
    private String classify(String key) {
        HashMap<String, Integer> values = keyValues.getValues(key);
        int dim;
        String unitOfMeasure;


        if (isNumerical(values)) // numerical datatype
        {
            dim = getDimension(values);
            if (dim > 4) {
                dim = 4;
            }

            unitOfMeasure = hasUnitOfMeasurement(values);

            if (unitOfMeasure != null) {
                return numberDim[dim] + unitOfMeasure;
            } else {
                return numberDim[dim];
            }
        } else // qualitative datatype (no numbers)
        {
            if (isBoolean(values)) {
                return bool;
            } else // undefined alphanumerical data >> universal
            {
                if (hasTextOnly(values)) {
                    return text;
                } else {
                    return universal;
                }

            }
        }
    }


    private String hasUnitOfMeasurement(HashMap<String, Integer> values) {
        // values are numeric
        Map<String, Integer> measurements = new HashMap<>();
        String v, unit;
        Integer oldCount = 0;
        int sum = 0;

        String result = "";

        for (Map.Entry<String, Integer> value : values.entrySet()) {
            v = value.getKey();
            sum = sum + value.getValue();

            v = v.replaceAll(firstNumReg, "");
            v = v.trim();

            unit = v.split(" ")[0];

            if (!measurements.containsKey(unit)) {
                measurements.put(unit, value.getValue());
            } else {
                oldCount = measurements.get(unit);
                measurements.put(unit, oldCount + value.getValue());
            }
        }

        for (Map.Entry<String, Integer> unitOfMeasure : measurements.entrySet()) {
            if (((double) unitOfMeasure.getValue() / sum) > beta) {
                if (unitOfMeasure.getValue().equals("")) {
                    return null;
                }
                result = unitOfMeasure.getKey();
            }
        }

        if (compounders.contains(result)) {
            return null;
        }
        return result;
    }

    /**
     * Returns the dimension of a set of values containing numericals, if not significantly determined returns 0;
     *
     * @param values
     * @return
     */
    private int getDimension(HashMap<String, Integer> values) {
        int[] dimCount = new int[10];

        Pattern pattern = Pattern.compile(numReg); // Regex to match int or float
        Matcher matcher;

        int count = 0, sum = 0;

        for (Map.Entry<String, Integer> v : values.entrySet()) {
            sum = sum + v.getValue();

            count = 0;
            matcher = pattern.matcher(v.getKey());
            while (matcher.find()) {
                count++;
            }

            if (count < 9) {
                dimCount[count] += v.getValue();
            } else {
                dimCount[9] += v.getValue();
            }
        }

        for (int i = 1; i < 10; i++) {
            if (((double) dimCount[i] / sum) > beta) {
                return i;
            }
        }
        return 0;
    }

    private boolean isBoolean(HashMap<String, Integer> values) {
        int count = 0;
        int sum = 0;

        String s;

        for (Map.Entry<String, Integer> e : values.entrySet()) {
            count = count + e.getValue();
            s = new String(e.getKey()).toLowerCase();
            s.trim();

            if (s.equals("yes") || s.equals("no")) {
                sum = sum + e.getValue();
            }
        }

        if (((double) sum) / count > beta) {
            return true;
        }
        return false;
    }

    private boolean isNumerical(HashMap<String, Integer> values) {
        Pattern firstNumericalPattern = Pattern.compile(firstNumReg); // Match int or float
        Matcher matcher;

        int count = 0;
        int sum = 0;

        for (Map.Entry<String, Integer> e : values.entrySet()) {
            count = count + e.getValue();
            matcher = firstNumericalPattern.matcher(e.getKey());

            if (matcher.find()) {
                sum = sum + e.getValue();
            }
        }

        if (((double) sum) / count > beta) {
            return true;
        }
        return false;
    }


    public boolean hasTextOnly(HashMap<String, Integer> values) {
        Pattern numericalPattern = Pattern.compile(numReg); // Match int or float
        Matcher matcher;

        int count = 0;
        int sum = 0;

        for (Map.Entry<String, Integer> value : values.entrySet()) {
            count = count + value.getValue();

            matcher = numericalPattern.matcher(value.getKey());
            if (!matcher.find()) {
                sum = sum + value.getValue();
            }
        }

        if (((double) sum) / count > beta) {
            return true;
        }
        return false;
    }


    public void printClassification() {
        HashMap<String, Integer> values;

        for (String key : keyValues.getKeys()) {
            values = keyValues.getValues(key);

            System.out.println("key: " + key + "\t occurance: " + keyValues.getKeyFreq(key));
            System.out.println("data type: " + dataTypes.get(key));
            System.out.print("values: ");

            for (Map.Entry<String, Integer> value : values.entrySet()) {
                System.out.print(value.getKey() + ": " + value.getValue() + " | ");
            }
            System.out.println("\n");
        }
    }


    public String getClassification(String key) {
        return dataTypes.get(key);
    }

    public String getNumReg() {
        return numReg;
    }

}

