package com.damirvandic.sparker.students.group6.UniformProductDescription;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wkuipers on 13-10-14.
 */
public class RegexTester {
    public static void main(String[] args) {
        String regex = "[0-9a-zA-Z\".-]+";
        Pattern numericalPattern = Pattern.compile(regex); // Match int or float
        Matcher matcher;

        int count = 0;
        int sum = 0;

        String e = "Toshiba 32\" Class (31.5\" Diag.) 720p 60Hz LED-LCD HDTV 32SL410U";
        String f = "Sony - BRAVIA / 40\" Class / 1080p / 60Hz / LCD HDTV";

        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(e);
        Matcher n = p.matcher(f);

        Set<String> modelWords = new HashSet<String>();

        while (m.find()) {
            modelWords.add(m.group());
        }

        print(modelWords);
    }

    public static void print(Set<String> set) {
        for (String o : set) {
            System.out.println(o);
        }
    }
}
