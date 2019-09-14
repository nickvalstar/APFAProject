package com.damirvandic.sparker.students.nick;


import java.util.*;

public class Key {


    public String key_name;
    public String webshop;
    public double coverage;
    public int diversity;
    public List<String> rawvalues = new ArrayList();
    public Set<String> rawuniquevalues;
    public List<String> possibleBrands =new ArrayList();
    public List<String> stringValues = new ArrayList();
    public List<Double> doubleValues = new ArrayList();
    public List<String> doubleValues_strings = new ArrayList<String>();
    public double productCountStrings;
    public double productCountDoubles;
    public String datatype;
    public String unit = "none";
    Map<String, Integer> cleaned_units = new HashMap();
    public Set<String> raw_units = new HashSet<String>();
    public double std;
    public int dimensions_two;
    public int dimensions_three;
    public String dimension;
    public int brandcount;


    Key(String webshop_input,String key_name_input){
        this.key_name = key_name_input;
        this.webshop = webshop_input;
        this.productCountStrings = 0;
        this.productCountDoubles = 0;
        this.dimensions_two=0;
        this.dimensions_three=0;
        this.dimension="";
        this.brandcount=0;
    }



    public void addRawValue(String input_value){
        rawvalues.add(input_value);
    }

    public void addStringValue(String input_value){
        if (!stringValues.contains(input_value))
            stringValues.add(input_value);
    }

    public void addDoubleValue(Double input_value){
        if (!doubleValues.contains(input_value))
            doubleValues.add(input_value);
    }

    public void calculateMetrics(Map<String, Double> webshop_sizes, Map<String, String> units, Set<String> exampleBrands){
        this.coverage = rawvalues.size()/webshop_sizes.get(this.webshop);//divide by its size
        this.rawuniquevalues = new HashSet<String>(rawvalues);
        this.diversity = rawuniquevalues.size();

        //dimensions:
        for (String value : this.rawvalues){
            int x = value.length() - value.replace(" x ", "  ").replace(" X ", "  ").length();
            if(x==0){
                continue;
            } else if(x==1){
                dimensions_two++;
            } else if(x==2){
                dimensions_three++;
            }
        }
        if ((double) dimensions_two > 0.9*(double) rawvalues.size() )
            this.dimension="_two_dimensional";
        else if ((double) dimensions_three > 0.9*(double) rawvalues.size() )
            this.dimension="_three_dimensional";

        //data type:
        if (this.productCountStrings > this.productCountDoubles) {
            this.datatype = "STRINGS";
        } else if (this.productCountDoubles>0 ){
            this.datatype = "DOUBLES"+this.dimension;
        } else {
            throw new IllegalArgumentException("Unknown value type: " + this.webshop + this.key_name);
        }
        for (Double d : this.doubleValues) {
            doubleValues_strings.add(d.toString()); //convert doubles to strings (for jaccard)
        }

        //units. decide which one is most frequent.
        int max_unit_count=0;
        for (String possible_unit : cleaned_units.keySet()){
            if(cleaned_units.get(possible_unit)>max_unit_count){
                this.unit= possible_unit;
                max_unit_count= cleaned_units.get(possible_unit);
            }
        }

        //std.dev
        std = get_standard_deviation();

        //check how many brands this key contains
        for (String b : this.rawvalues){
            if (b.contains(" "))
                b = b.substring(0,b.indexOf(" ")); //take only first word.
            this.possibleBrands.add(b.toLowerCase());
        }
        List<String> checkedBrands = new ArrayList(possibleBrands);
        checkedBrands.retainAll(exampleBrands);
        this.brandcount = checkedBrands.size();
//        if (this.brandcount>0){
//            System.out.println(this.key_name + "  " + this.brandcount);
//            for (String str : this.possibleBrands){
//                System.out.println(str);
//            }
//            System.out.println("-------");
//        }
    }

    public void addCleanedUnit(String found_unit) {
        if (!cleaned_units.containsKey(found_unit)){
            cleaned_units.put(found_unit,0);
        }
        cleaned_units.put(found_unit, cleaned_units.get(found_unit) + 1);
    }

    public void addRawUnit(String found_unit) {
        if (!raw_units.contains(found_unit)){
            raw_units.add(found_unit);
        }
    }

    public double get_standard_deviation(){
        if (doubleValues.size()<=1)
            return 0;
        double sum = 0.0;
        for(double a : doubleValues)
            sum += a;
        double mean = sum/doubleValues.size();
        double temp = 0;
        for(double a : doubleValues)
            temp += (mean-a)*(mean-a);
        double var = temp/doubleValues.size();
        return Math.sqrt(var);
    }
}
