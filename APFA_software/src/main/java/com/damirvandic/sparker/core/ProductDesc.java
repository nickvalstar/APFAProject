package com.damirvandic.sparker.core;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This immutable class represents one product description from the JSON input file.
 */
public final class ProductDesc implements Serializable {
    public final int ID;
    public final String strRepr;
    public final String title;
    public final String modelID;
    public final String url;
    public final String shop;
    public final Map<String, String> featuresMap;

    public ProductDesc(int ID,
                       String title,
                       String modelID,
                       String url,
                       Map<String, String> featuresMap) {
        assert (title != null);
        assert (modelID != null);
        assert (url != null);
        assert (featuresMap != null);
        String shop = extractShop(url);
        assert (!shop.equals(url)); // if url is not a url, this will cause an error

        this.ID = ID;
        this.strRepr = String.format("P[%d]", ID);
        this.title = title;
        this.modelID = modelID;
        this.url = url;
        this.shop = shop;
        this.featuresMap = Collections.unmodifiableMap(new HashMap<>(featuresMap));
    }

    public static String extractShop(String url) {
        return getBaseDomain(url);
    }

    /**
     * Will take a url such as http://www.stackoverflow.com and return www.stackoverflow.com
     *
     * @param url
     * @return
     */
    private static String getHost(String url) {
        if (url == null || url.length() == 0)
            return "";

        int doubleslash = url.indexOf("//");
        if (doubleslash == -1)
            doubleslash = 0;
        else
            doubleslash += 2;

        int end = url.indexOf('/', doubleslash);
        end = end >= 0 ? end : url.length();

        return url.substring(doubleslash, end);
    }

    /**
     * Based on : http://grepcode.com/file/repository.grepcode.com/java/ext/com.google.android/android/2.3.3_r1/android/webkit/CookieManager.java#CookieManager.getBaseDomain%28java.lang.String%29
     * Get the base domain for a given host or url. E.g. mail.google.com will return google.com
     *
     * @param url
     * @return
     */
    private static String getBaseDomain(String url) {
        String host = getHost(url);

        int startIndex = 0;
        int nextIndex = host.indexOf('.');
        int lastIndex = host.lastIndexOf('.');
        while (nextIndex < lastIndex) {
            startIndex = nextIndex + 1;
            nextIndex = host.indexOf('.', startIndex);
        }
        if (startIndex > 0) {
            return host.substring(startIndex);
        } else {
            return host;
        }
    }

    @Override
    public String toString() {
        return strRepr;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ProductDesc)) return false;
        ProductDesc other = (ProductDesc) obj;
        return other.modelID.equals(this.modelID) && other.title.equals(this.title);
    }

    @Override
    public int hashCode() {
        return this.ID;
    }
}
