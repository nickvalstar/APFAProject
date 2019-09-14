package com.damirvandic.sparker.students.nick;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ProductInfo {

    public String titleBrand = "unknown";
    public Map<String,Set<Double>> titleUnits = new HashMap();
    public Set<String> titleRest = new HashSet<>();

    ProductInfo(){
    }

    public void setBrand(String brand){
        this.titleBrand = brand;
    }


    public void addUnit(String unit, double value){
        if (value==-1) //no value
            return;
        if (!titleUnits.containsKey(unit))
            titleUnits.put(unit,new HashSet<Double>());
        if (!titleUnits.get(unit).contains(value))
            titleUnits.get(unit).add(value);
    }

    public void addRest(String rest){
        String cleaned_rest = rest.replace(",", "")
                .replace("\\", "")
                .replace("/", "")
                .replace(":", "")
                .replace("~", "")
                .replace("-", "")
                .toLowerCase();
        if (!titleRest.contains(cleaned_rest))
            titleRest.add(cleaned_rest);
    }


}
