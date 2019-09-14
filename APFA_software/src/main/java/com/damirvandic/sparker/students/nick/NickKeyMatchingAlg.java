package com.damirvandic.sparker.students.nick;

import java.io.FileWriter;
import java.io.IOException;
import com.damirvandic.sparker.core.ProductDesc;

import com.damirvandic.sparker.msm.TitleAnalyzer;
import com.damirvandic.sparker.msm.TitleAnalyzerImpl;
import com.damirvandic.sparker.util.StringPair;
import com.google.common.primitives.Doubles;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.commons.math3.stat.inference.TTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.shef.wit.simmetrics.similaritymetrics.AbstractStringMetric;
import uk.ac.shef.wit.simmetrics.similaritymetrics.QGramsDistance;
import uk.ac.shef.wit.simmetrics.tokenisers.InterfaceTokeniser;
import uk.ac.shef.wit.simmetrics.tokenisers.TokeniserQGram2;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class NickKeyMatchingAlg {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final KolmogorovSmirnovTest km = new KolmogorovSmirnovTest();
    private final TTest studentTest = new TTest();
    private Pattern leestekenDetector = Pattern.compile("([a-zA-Z])+");
    private static Pattern numberDetector = Pattern.compile("((?:\\d*\\.)?\\d+)");
    public static Map<String,Map> alignments = new HashMap();
    public static Map<String,Map> alignments2 = new HashMap();
    public static Map<String,Map> alignments_test = new HashMap();
    public static Map<String,Map> alignments2_test = new HashMap();
    private static int testing;
    private static Map<String, Map> keys = new HashMap();
    private static final Pattern MW_PATTERN = getMWPattern();
    private static InterfaceTokeniser tokeniser = new TokeniserQGram2();
    private static AbstractStringMetric metric = new QGramsDistance(tokeniser);
    public static int testcount=0;
    public static Map<String, Double> best_params = new HashMap();
    public static Map<String,Set<String>> brands = new HashMap();
    public static Map<String,String> brandkeys = new HashMap();
    public static Map<Integer,ProductInfo> productInfoMap = new HashMap();
    public Map<String,String> units = new HashMap();
    public static int total_counter=0;
    public static int msm_counter=0;
    public static int apfa_counter=0;
    private static double mu;
    private static double aligned_min;
    private static double title_min;
    private static double keys_rest_weight;
    private static double title_rest_weight;
    private static double epsilon;
    private static double epsilon2;

    public NickKeyMatchingAlg(Map<String, Object> conf, Set<ProductDesc> data) {
        this.mu = (Double) conf.get("mu");
        this.aligned_min = (double) conf.get("aligned_min");
        this.title_min = (double) conf.get("title_min");
        this.keys_rest_weight = (double) conf.get("keys_rest_weight");
        this.title_rest_weight = (double) conf.get("title_rest_weight");
        this.epsilon = (double) conf.get("epsilon");
        this.epsilon2 = (double) conf.get("epsilon2");
        computeAnswers(conf, data);
    }




    private void computeAnswers(Map<String, Object> conf, Set<ProductDesc> data) {


        //settings
        boolean onebootstrap=false;
        boolean phase0_testing=false;
        boolean onlyphase2=false;
        //set w1 en w2 2x goed. in alg 3
        //settings


        System.out.println("running phase0");
        testcount++;
        if (data.size()>900){
            testing=0;
        }
        else{
            testing=1;
        }

        if (onebootstrap==true){
            if(testing==0 & !alignments.isEmpty()){
                System.out.println("training was already done");
                return;
            }
            if(testing==1 & !alignments_test.isEmpty()){
                System.out.println("testing was already done");
                return;
            }
        }

        if (testing==0)
            System.out.println("training");
        else
            System.out.println("testing");

        //initialize:
        keys.clear();

        //todo unit INPUT: (should read this in from csv!)
        Map<String,List<String>> units_input = new HashMap();
        units_input.put("inch",Arrays.asList("inch","inches","-inch"));
        units_input.put("lb",Arrays.asList("lb","lbs","pound","pounds"));
        units_input.put("hz",Arrays.asList("hz","hertz"));
        units_input.put("mwatt",Arrays.asList("mw"));
        units_input.put("watt",Arrays.asList("watt","w","watts"));
        units_input.put("volt",Arrays.asList("volt","v"));
        units_input.put("ms",Arrays.asList("ms"));
        units_input.put("nit",Arrays.asList("nit","nt","cd"));
        units_input.put("year",Arrays.asList("year","years"));
        units_input.put("day",Arrays.asList("day","days"));
        units_input.put("hour",Arrays.asList("hour","hours","hrs"));
        units_input.put("month",Arrays.asList("month","months"));
        units_input.put("week",Arrays.asList("week","weeks"));
        units_input.put("kg",Arrays.asList("kg","kilo"));
        units_input.put("mm",Arrays.asList("mm"));
        units_input.put("degrees",Arrays.asList("degrees","degree","º"));
        units_input.put("dollar",Arrays.asList("$"));
        units_input.put("feet",Arrays.asList("feet","ft"));
        units_input.put("pixels",Arrays.asList("p"));
        units_input.put("pixels_interlaced", Arrays.asList("i"));
        units_input.put("dimension", Arrays.asList("d","-d"));


        for (String unit_name : units_input.keySet()){
            List<String> unit_values = units_input.get(unit_name);
            for (String unit_value : unit_values){
                units.put(unit_value,unit_name);
            }
        }

        Map<String, Double> webshop_sizes = new HashMap();


        //Aggregate per webshop per key all values.
        for (ProductDesc p : data) {
            //add shop
            String shop = p.shop;
            if (!webshop_sizes.containsKey(shop)){
                webshop_sizes.put(p.shop,0.0);
            }
            webshop_sizes.put(p.shop,webshop_sizes.get(p.shop)+1.0);
            if (!keys.containsKey(shop)){
                keys.put(shop, new HashMap<String, Key>());
            }

            for (String feat : p.featuresMap.keySet()){//go through keys
                if (feat==null || feat=="")
                    System.out.println("empty feat");
                if(!keys.get(shop).containsKey(feat)){
                    keys.get(shop).put(feat,new Key(shop,feat));
                }
                Key key = (Key) keys.get(shop).get(feat);
                if (key==null)
                    System.out.println("empty key");
                //add raw value (but without info between brackets)
                String value = p.featuresMap.get(feat);
                if (value.contains("(")){ //everything between brackets is much less important:
                    value = value.substring(0,value.indexOf("("));
                }
                if (value==null || value=="")
                    System.out.println("empty value");
                key.addRawValue(value);

                //add splitted and cleaned value
                String[] splits = value
                        .replace(",", "")//e.g. in milions. 1,000,000 should be just 1000000
                        .replace("x", " ")
                        .replace("$", "dollar")
                        .replace("\"", "inch")
                        .replace("º","degrees")
                        .replace("/", " ")
                        .replace("”", "inch")
                        .replace(":", " ")
                        .replace("~", " ")
                        .replace("-", " ").split(" +|;");
                for (String waardeSplitted : splits) {
                    String foundValue = "";
                    String foundString = "";
                    Matcher getallen = numberDetector.matcher(waardeSplitted);

                    //System.out.println(waardeSplitted);
                    //System.out.println();
//                    if(waardeSplitted.contains("lbs.")){
//                        int j = 1;
//                    }
                    if (getallen.find()) {
                        foundValue = findGetallen(getallen);
                        double converted = Double.parseDouble(foundValue);
                        key.addDoubleValue(converted);
                        key.productCountDoubles = key.productCountDoubles + 1;
                        waardeSplitted = waardeSplitted.replaceAll("\\d",""); //strip the numbers
                    }

                    Matcher strings = leestekenDetector.matcher(waardeSplitted);
                    if (strings.find()) {
                        strings = leestekenDetector.matcher(waardeSplitted); //renew it
                        foundString = findStrings(strings, foundString);

                        //units
                        if (units.containsKey(foundString)){
                            key.addRawUnit(foundString);
                            key.addCleanedUnit(units.get(foundString));
                            key.productCountDoubles = key.productCountDoubles + 0.25; //bonus to doubles.
                            //dont add it as a string value, because containing a unit rather suggests we are dealing with a key of type double.
                        } else{
                            key.addStringValue(foundString);
                            key.productCountStrings = key.productCountStrings + 1;
                        }
                    }
                }
            }
        }



        //todo brand INPUT: (should read this in from csv!)
        Set<String> exampleBrands = new HashSet<>(Arrays.asList(new String[]{"samsung", "philips", "sony", "sharp", "nec", "hp", "toshiba", "hisense", "sony", "lg", "sanyo", "coby", "panasonic", "sansui", "vizio", "viewsonic", "sunbritetv", "haier", "optoma", "proscan", "jvc", "pyle", "sceptre", "magnavox", "mitsubishi", "supersonic", "compaq", "hannspree", "upstar", "seiki", "rca", "craig", "affinity", "naxa", "westinghouse", "epson", "elo", "sigmac"}));


        //calculate individual metrics per webshop per key (coverage, diversity,datatype,units)
        for (String webshop : keys.keySet()){
            int highestbrandCount=0;
            Key brandkey = null;
            for (Object keyobject : keys.get(webshop).values()){
                Key key = (Key) keyobject;
                key.calculateMetrics(webshop_sizes,units, exampleBrands);
                //which key contains the brands?
                if (key.brandcount>highestbrandCount){
                    highestbrandCount = key.brandcount;
                    brandkey = key;
                }
            }
            //gather brands per webshop
            if (highestbrandCount>=0){
                brands.put(webshop,new HashSet<String>(brandkey.possibleBrands));
                brandkeys.put(webshop,brandkey.key_name);
            }
        }

        if (phase0_testing!=true){
            //int good = 0;
            //int bad = 0;
            //PRODUCT_INFO:  voor train en test
            //todo (should read this in from csv!)
            String[] extendedBrandlist = {"DYNEX","GPX","VENTURER","TCL","AZEND","HITEKER","VIORE","CONTEX","INSIGNIA","MITSUBISHI","JVC","PROSCAN","HISENSE","UPSTAR","WESTINGHOUSE","SANYO","SIGMAC","RCA","HANNSPREE","SCEPTRE","PHILIPS","SAMSUNG","NEC","LG","PANASONIC","SANSUI","COBY","VIZIO","VIEWSONIC","TOSHIBA","SUNBRITETV","VIZIO","SHARP","HAIER","SUPERSONIC","JVC","PYLE","MAGNAVOX","COMPAQ","HANNSPREE","SONY","AFFINITY","SEIKI","NAXA","EPSON","HP","ELO","ACEAWTASD","ACER","ADMIRAL","ADVENT","ADYSON","AIWA","AKAI","AKURRA","ALBA","ALLORGAN","AMIGO VIDEO","AMPLIVISION","AMSTRAD","ANDREA ELECTRONICS","ANITECH","ARCAM","ARENA","ARGOSY","ASTOR","ASUKA","ATLANTIC","ATWATER TELEVISION","AUDAR","AUDIOSONIC ","AUDIOVOX","AWA","BACE TELEVISION","BAIRD","BANG & OLUFSEN","BASE","BAUR","BAYCREST ","BEKO","BENQ","BELL TELEVISION","BEON","BINATONE","BISA","BLAUPUNKT","BLUE SKY","BLUE STAR","BONDSTEC","BPL","BRANDT","BRIONVEGA","BRITANNIA","BSR","BTC","BUSH RADIO","CALBEST ELECTRONICS","CANDLE","CAPEHART ELECTRONICS","CARREFOUR","CASCADE","CATHAY","CELLO","CENTURION","CGE[DISAMBIGUATION NEEDED]","CHANGHONG[DISAMBIGUATION NEEDED]","CHIMEI","CIMLINE","CITIZEN","CLAIRETONE ELECTRIC CORPORATION","CLARIVOX","CLATRONIC","COBY","COLONIAL RADIO ","COLOR ELECTRONICS CORPORATION","CONAR INSTRUMENTS","CONDOR","CONRAC ","CONRAC ","CONTEC","CONTINENTAL EDISON","CORTRON INDUSTRIES ","COSSOR","CROWN","CRYSTAL","CTC","CURTIS MATHES CORPORATION","CYBERTRON","DAEWOO","DAINICHI","DANSAI","DAYTON","GRAAF","DECCA","DECCACOLOUR ","DEFIANT","DELL","DIAMOND VISION","DELMONICO INTERNATIONAL CORPORATION","DIBOSS","DIXI","DUMONT LABORATORIES ","DUMONT ","DURABRAND ","DYANORA ","DYNATRON","EKCO","ELBE","ELECTROHOME","ELEMENT ELECTRONICS","ELIN","ELITE","ELTA","EMERSON RADIO AND PHONOGRAPH","EMERSON ","EMI","ERRES","EXPERT","FARNSWORTH TELEVISION AND RADIO","FERGUSON ELECTRONICS","FERRANTI","FIDELITY RADIO ","FINLANDIA","FINLUX","FIRSTLINE","FISHER ELECTRONICS","FLINT","FORMENTI","FRONTECH ","FUJITSU","FUNAI ELECTRIC","GELOSO","GENERAL ELECTRIC","GENERAL ELECTRIC COMPANY PLC ","GELOSO","GENEXXA","GOLDSTAR","GOODMANS INDUSTRIES","GORENJE","GPM","GRADIENTE","GRAETZ","GRANADA","GRANDIN","GRUNDIG","HANNSPREE","HANSEATIC","HANTAREX","HARVARD INTERNATIONAL","HARVEY INDUSTRIES","HCM","HEALING","HEATH COMPANY/HEATHKIT","HINARI DOMESTIC APPLICANES","HISAWA","HMV","HISENSE[DISAMBIGUATION NEEDED]","HITACHI, LTD.","HOFFMAN TELEVISION ","HOWARD RADIO ","HUANYU","HYPSON","ICE","ICES","ITS","ITT CORPORATION","ITT-KB ","ITT-SEL ","IMPERIAL","INDIANA","INGELEN","INNO HIT","INTERFUNK","INTERVISION","ISUKAI","IZUMI","JENSEN LOUDSPEAKERS","JMB","JVC","KAISUI","KAMACROWN","KANE ELECTRONICS CORPORATION","KAPSCH","KATHREIN","KENDO","KENT TELEVISION","KINGSLEY","KLOSS VIDEO","KNEISSEL","KOGAN","KOLSTER-BRANDES","KONKA","KORPEL","KOYODA","LANIX","LEYCO","LG ELECTRONICS ","LIESENKÖTTER ","LLOYDS","LOEWE AG","LUMA","LUXOR","MTC","MAGNADYNE","MAGNAFON","MAGNASONIC","MAGNAVOX","MAGNAVOX ","MANETH","MARANTZ","MARCONIPHONE","MARK","MATSUI","MATTISON ELECTRONICS","MCMICHAEL RADIO","MEDIATOR ","MEMOREX","MICROMAX","MERCURY-PACIFIC","METZ","MINERVA","MINOKA","MIRC ELECTRONICS ","MITSUBISHI","MIVAR","MOTOROLA","MULTITECH ","MUNTZ ","MURPHY RADIO","NEC","NECKERMANN","NELCO ","NEI","NETTV","NIKKAI","NOBLIKO","NOKIA ","NORDMENDE","NORTH AMERICAN AUDIO, INC.","OLYMPIC RADIO AND TELEVISION","OCEANIC","ONIDA","ONWA","ORION ","ORION ","OSAKI","OSO","OSUME","OTAKE","OTTO VERSAND","PACKARD BELL, TELEDYNE PACKARD BELL","PALLADIUM","PANAMA","MATSUSHITA ","PATHE CINEMA","PATHE MARCONI","PAUSA","PERDIO","PETO SCOTT","PHILCO ","PHILIPS","PHILMORE MANUFACTURING","PHONOLA","PILOT RADIO","PILOT RADIO CORPORATION ","PIONEER ELECTRONICS","PLANAR SYSTEMS","POLAROID","PROFEX","PRIMA","PROLINE","PROSCAN ","PROTECH","PULSER ","QUASAR","PYE","PYXSCAPE","QUELLE","QUESTA","REI","RADIOLA","RADIOLA ","RADIOMARELLI","RADIOSHACK","RANK ARENA","RAVENSWOOD ","RAULAND BORG","RBM","RCA","REALISTIC","REDIFFUSION","REGENTONE","REVOX","REX","RFT ","RGD ","ROADSTAR ","ROLLS","SABA","SACCS","SAISHO","SALORA","SALORA INTERNATIONAL","SAMBERS","SAMPO CORP. OF AMERICA","SAMSUNG","SANABRIA TELEVISION CORPORATION ","SANDRA","SANSUI","SANYO","SBR","SCHAUB LORENZ","SCHNEIDER ELECTRIC","SCHNEIDER ","SCHNEIDER ","SEARS ","SEG","SEI","SEI-SINUDYNE","SEIKI DIGITAL","SELCO ","SÈLECO","SENTRA","SETCHELL CARLSON","SEURA[2]","SHARP","SHORAI","SIAREM","CURTISYOUNG","SIEMENS AG","SILO DIGITAL","SILVER","SILVERTONE - ","SINUDYNE","SKYWORTH[DISAMBIGUATION NEEDED]","SOBELL","SOLAVOX","SONITRON","SONOKO","SONOLOR","SONORA","SONTEC","SONY","SOYO","SOUNDWAVE","STANDARD","STERN","STROMBERG CARLSON","STEWART-WARNER","SUNLITE TV","SUNKAI","SUSUMU","SUPRA","SYLVANIA","SYMPHONIC ELECTRONIC CORP","SYMPHONIC RADIO AND ELECTRONICS","SYSLINE","TANDY","TASHIKO","TATUNG COMPANY","TCL[DISAMBIGUATION NEEDED]","TEC","TECH-MASTER","TECHNEMA","TECHNICS","TECHNISAT","TECNIMAGEN ","TECHNIKA","TECO","TELEAVIA","TELEFUNKEN","TELEMEISTER","TELEQUIP","TELETECH","TELETON","TELETRONICS","TENSAI","TEXET","THOMSON SA ","THORN ELECTRICAL INDUSTRIES","THORN EMI","TMA","TOMASHI","TOSHIBA","TPV TECHNOLOGY","TP VISION ","TRANSVISION","TRAV-LER RADOP","TRAVELERS ELECTRONICS CO.","TRINIUM ELECTRONICS PHILIPPINES","TRIUMPH","UHER","ULTRA","ULTRAVOX","UNITED STATES TELEVISION MANUFACTURING CORP.","UNIVERSUM","VESTEL","VIDEOCON","VIDEOSAT","VIDEOTECHNIC","VIEWSONIC","VISION","VISTRON","VIZIO","VU","WALTHAM","WARWICK","WATSON","WATT RADIO","WELLS-GARDNER ","WESTINGHOUSE","WESTINGHOUSE DIGITAL","WESTON ELECTRONICS ","WHITE-WESTINGHOUSE","X2GEN","YOKO","YOKO ","ZANUSSI","ZENITH RADIO","ZONDA"};
            Set<String> extendedBrands = new HashSet<>();
            for (String b : extendedBrandlist){
                if (b.contains(" "))
                    b = b.substring(0,b.indexOf(" ")); //take only first word.
                if (!extendedBrands.contains(b.replace("\u0099", "").toLowerCase()))
                    extendedBrands.add(b.replace("\u0099","").toLowerCase());
            }
            for (ProductDesc p : data) {
                if (productInfoMap.containsKey(p.ID))
                    continue; //already have this product's info
                ProductInfo pInfo = new ProductInfo();
                productInfoMap.put(p.ID, pInfo);
                //-----------BRAND
                if (!brands.containsKey(p.shop))
                    System.out.println("-----------------------------ERROR");
                Set<String> tokens1 = new HashSet<>(Arrays.asList(p.title.replace("\u0099", "").replace(",", " ").toLowerCase().split("\\s+")));
                tokens1.retainAll(brands.get(p.shop));    //retainAll takes intersection of (by definition unique) items of both sets
                if (!tokens1.isEmpty()){
                    if (tokens1.size()>1)
                        System.out.println("Multiple brands found");
                    String onetoken1 = tokens1.iterator().next();
                    pInfo.setBrand(onetoken1);
                } else {
                    //search first in key
                    if (brandkeys.containsKey(p.shop) & p.featuresMap.containsKey(brandkeys.get(p.shop))){
                        String foundBrand = p.featuresMap.get(brandkeys.get(p.shop));
                        if (foundBrand.contains(" "))
                            foundBrand = foundBrand.substring(0,foundBrand.indexOf(" ")); //take only first word.
                        foundBrand = foundBrand.replace("\u0099","").toLowerCase();
                        brands.get(p.shop).add(foundBrand);
                        pInfo.setBrand(foundBrand);
                    } else { //search in extended brand list
                        Set<String> tokens2 = new HashSet<>(Arrays.asList(p.title.replace("\u0099", "").replace(",", " ").toLowerCase().split("\\s+")));
                        tokens2.retainAll(extendedBrands);
                        if (!tokens2.isEmpty()){
                            String onetoken2 = tokens2.iterator().next();
                            if (tokens2.size()>1)
                                System.out.println("Multiple brands found");
                            brands.get(p.shop).add(onetoken2);
                            pInfo.setBrand(onetoken2);
                        } else{
                            System.out.println(p.title);
                        }
                    }
                }
                //---------TITLE
                String t = p.title;

                Set<String> mws = extractModelWords(t);
                for (String mw : mws){
                    String[] splits = mw
                            .replace(",", "")//e.g. in milions. 1,000,000 should be just 1000000
                            .replace("x", " ")
                            .replace("$", "dollar")
                            .replace("\"", "inch")
                            .replace("º", "degrees")
                            .replace("/", " ")
                            .replace(":", " ")
                            .replace("~", " ")
                            .replace("”", "inch")
                            .replace("-", "").split(" +|;");

                    for (String waardeSplitted : splits) {
                        String foundValue = "";
                        String foundString = "";
                        Matcher getallen = numberDetector.matcher(waardeSplitted);
                        double converted = -1;
                        if (getallen.find()) {
                            foundValue = findGetallen(getallen);
                            converted = Double.parseDouble(foundValue);
                            waardeSplitted = waardeSplitted.replaceAll("\\d",""); //strip the numbers
                        }

                        Matcher strings = leestekenDetector.matcher(waardeSplitted);
                        if (strings.find()) {
                            strings = leestekenDetector.matcher(waardeSplitted); //renew it
                            foundString = findStrings(strings, foundString);

                            //units
                            if (units.containsKey(foundString)){
                                String canonical = units.get(foundString);
                                pInfo.addUnit(canonical,converted);
                            } else {
                                pInfo.addRest(mw);
                            }
                        }
                    }
                }
            }
        }


        if (onlyphase2) //always
            return;

        if(testing==0){

            try
            {
                FileWriter writer = new FileWriter("./hdfs/nick_phase0_training.csv",true);
                writer.write("f1;precision;recall;similarity_threshold;min_key_score;ttest_multiplier;string_bonus;max_contained_score;key_weight;" +
                        "double_weight;string_weight;cov_weight;div_weight;unit_weight;counter");
                writer.append('\n');
                writer.flush();

                //initialization:
                boolean assigning;
                List<String> checked_shops = new ArrayList<>();
                double highest_f1=-1;
                Key best_pair_k1;
                Key best_pair_k2;
                Key best_matching_k2;
                double highest_pair_score;
                double highest_k2_score;
                double p_value;
                double double_score;
                double string_score;
                double double_jaccard_score;
                double ttest_score;
                double unit_score;
                double div_score;
                double cov_score;
                double key_score;
                boolean key_contains1;
                boolean key_contains2;
                double is_string;
                int counter=0;
                Double[] f;
                double f1;
                double precision;
                double recall;
//                String w1 = "newegg.com";
//                String w2 = "bestbuy.com";

                //some parameters:
                //double ttest_multiplier=2;
//                double similarity_threshold=1.7;
//                double min_key_score=0.7;
//                double string_bonus=0.4;
//                double max_contained_score=0.75;
////
                //some weight parameters:
//                double key_weight       = 1.1;
//                double double_weight    = 2;
//                double string_weight    = 2;
//                double cov_weight       = 0.5;
//                double div_weight       = 0.5;
//                double unit_weight      = 0.5;


                Double[] similarity_thresholds  =new Double[]{1.7};//1.7 ook 1.5-1.7
                Double[] min_key_scores         =new Double[]{0.7};//0.7 echt
                Double[] ttest_multipliers      =new Double[]{2.0};//2 is geen variabele meer.
                Double[] string_bonuss          =new Double[]{0.4};//0.4. ook 0.2-0.6
                Double[] max_contained_scores   =new Double[]{0.75};//0.7

                Double[] key_weights            =new Double[]{1.1};//1.1 of 1 of 0.9
                Double[] double_weights         =new Double[]{5.0};//2 ook 1.75-2.5
                Double[] string_weights         =new Double[]{2.0};//2 ook 2-2.5
                Double[] cov_weights            =new Double[]{0.5};//0.5 ook 0.2 -0.8
                Double[] div_weights            =new Double[]{0.5};//0.5 ook 0-0.5
                Double[] unit_weights           =new Double[]{0.5};//0.5 alles

                long start_time = System.currentTimeMillis();

                for (double ttest_multiplier : ttest_multipliers){ //2
                    for (double key_weight : key_weights){ //1.1
                        for (double max_contained_score : max_contained_scores){//0.75
                            for (double min_key_score : min_key_scores){//0.7
                                for (double cov_weight : cov_weights){ //0.5
                                    for (double div_weight : div_weights){ //0.5
                                        for (double unit_weight : unit_weights){ //0.5
                                            for (double string_bonus : string_bonuss){ //0.4
                                                for (double string_weight : string_weights){ //2
                                                    for (double similarity_threshold : similarity_thresholds){ //1.7
                                                        for (double double_weight : double_weights){ //2

                                                            counter++;
                                                            //System.out.println(counter);
                                                            if(counter % 12 == 0){
                                                                long end_time = System.currentTimeMillis();
                                                                long difference = end_time-start_time;
                                                                System.out.println(counter);
                                                                System.out.println(difference/1000 + " seconds.");
                                                            }

                                                            alignments.clear();
                                                            //alignments2.clear();
                                                            //allign keys between webshops
                checked_shops.clear();
                for (String w1 : keys.keySet()){ //loop over webshops
                    checked_shops.add(w1);
                    for (String w2 : keys.keySet()){
                        if(checked_shops.contains(w2)){continue;} //to prevent double comparisons

                                                            Set<Object> keys1 = new HashSet(keys.get(w1).values());
                                                            Set<Object> keys2 = new HashSet(keys.get(w2).values());

                                                            //double f1_msm = testmsm(keys1,keys2,0.7);

                                                            assigning = true;
                                                            while(assigning){
                                                                assigning=false;
                                                                best_pair_k1 = null;
                                                                best_pair_k2 = null;
                                                                highest_pair_score=-1;
                                                                for (Object keyobject1 : keys1){
                                                                    Key k1 = (Key) keyobject1; //take one key from webshop 1
                                                                    best_matching_k2 = null;
                                                                    highest_k2_score=-1;

                                                                    for (Object keyobject2 : keys2){
                                                                        Key k2 = (Key) keyobject2; //loop over all keys from webshop 2

                                                                        if (!k1.datatype.equals(k2.datatype))
                                                                            continue; //we dont even score it.

                                                                        p_value = 0;
                                                                        double_score=0;
                                                                        string_score=0;
                                                                        double_jaccard_score=0;
                                                                        ttest_score=0;
                                                                        unit_score=0;
                                                                        div_score=0;
                                                                        cov_score =0;
                                                                        key_score =0;
                                                                        key_contains1=false;
                                                                        key_contains2=false;
                                                                        is_string=0;

                                                                        //key_name
                                                                        key_score = metric.getSimilarity(preProcess(k1.key_name),preProcess(k2.key_name));
                                                                        key_contains1 = preProcess(k1.key_name).contains(preProcess(k2.key_name));
                                                                        key_contains2 = preProcess(k2.key_name).contains(preProcess(k1.key_name));
                                                                        if (key_contains1 | key_contains2){
                                                                            key_score = java.lang.Math.max(key_score,max_contained_score);
                                                                        }
                                                                        if(key_score<min_key_score){continue;} //too risky

                                                                        if (k1.datatype.contains("DOUBLES")) {
                                                                            double[] valuesA = Doubles.toArray(k1.doubleValues);
                                                                            double[] valuesB = Doubles.toArray(k2.doubleValues);

                                                                            if (valuesA.length > 1 && valuesB.length > 1) {
                                                                                //double p_value = km.kolmogorovSmirnovTest(valuesA, valuesB, true);
                                                                                p_value = studentTest.tTest(valuesA, valuesB);
                                                                                ttest_score=p_value;
                                                                            }
                                                                            double_jaccard_score = jaccard_similarity(k1.doubleValues_strings, k2.doubleValues_strings);
                                                                            //System.out.println(ttest_score + "__" + double_jaccard);
                                                                            double_score = java.lang.Math.max(ttest_score,double_jaccard_score);
                                                                            double_score = java.lang.Math.min(double_score,1);


                                                                            if (k1.unit.equals("none") | k2.unit.equals("none")){
                                                                                unit_score = 0;
                                                                            } else {
                                                                                if (k1.unit.equals(k2.unit)) {
                                                                                    unit_score = 1;
                                                                                } else {
                                                                                    unit_score = -1;
                                                                                }
                                                                            }
                                                                        } else { //STRINGS
                                                                            string_score = jaccard_similarity(k1.stringValues, k2.stringValues);
                                                                            is_string = 1;
                                                                        }

                                                                        //coverage
                                                                        cov_score= -java.lang.Math.pow(k1.coverage - k2.coverage, 2.0); //negative squared error
                                                                        //diversity: if one of their diversities is only 1, punish.
                                                                        if (java.lang.Math.min(k1.diversity,k2.diversity)==1){
                                                                            div_score=-1;
                                                                        }

                                                                        double total_score =
                                                                                key_weight      * key_score +
                                                                                        double_weight   * double_score +
                                                                                        string_weight   * string_score +
                                                                                        cov_weight      * cov_score +
                                                                                        div_weight      * div_score +
                                                                                        unit_weight     * unit_score +
                                                                                        is_string       * string_bonus;

                                                                        //which k2 matches this k1 best?
                                                                        if (total_score>highest_k2_score){
                                                                            highest_k2_score=total_score;
                                                                            best_matching_k2=k2;
                                                                        }

                                                                    }

                                                                    //which k1 has the best match?
                                                                    if (highest_k2_score>highest_pair_score){
                                                                        highest_pair_score=highest_k2_score;
                                                                        best_pair_k1 = k1;
                                                                        best_pair_k2 = best_matching_k2;
                                                                    }

                                                                }

                                                                //assign k2 to k1 in 1.

                                                                if (highest_pair_score>=similarity_threshold ){ //has to be higher than some threshold.
                                                                    addMatchingKey(alignments,w1, w2, best_pair_k1.key_name, best_pair_k2.key_name);
                                                                    //todo maybe later i should create alignments2 from alignments after this loop.
                                                                    addMatchingKey(alignments2, best_pair_k1, best_pair_k2, highest_pair_score);
                                                                    // and remove them from pool.
                                                                    keys1.remove(best_pair_k1);
                                                                    keys2.remove(best_pair_k2);
                                                                    assigning=true;
                                                                }
                                                            }
                                                            //deze 2 voor alle shops:
                                                            }
                                                            }
                                                            // deze 2 voor alle shops
                                                            f = hitmatrix_2shops();
                                                            f1 = f[0];
                                                            precision = f[1];
                                                            recall = f[2];
                                                            //System.out.println(f1);
                                                            if (f1 > highest_f1){
                                                                highest_f1=f1;
                                                                //store it
                                                                String str = f1+";"+precision+";"+recall+";"+similarity_threshold+";"+min_key_score+";"+ttest_multiplier+";"+string_bonus+";"+
                                                                        max_contained_score+";"+key_weight+";"+double_weight+";"+string_weight+";"+cov_weight+";"+div_weight+";"+unit_weight+";"+counter;
                                                                System.out.println(str);
                                                                writer.write(str);
                                                                writer.append('\n');
                                                                writer.flush();
                                                                best_params.clear();
                                                                best_params.put("f1_train",f1);
                                                                best_params.put("similarity_threshold", similarity_threshold );
                                                                best_params.put("min_key_score", min_key_score );
                                                                best_params.put("ttest_multiplier",ttest_multiplier);
                                                                best_params.put("string_bonus",string_bonus);
                                                                best_params.put("max_contained_score",max_contained_score);

                                                                //weights
                                                                best_params.put("key_weight", key_weight );
                                                                best_params.put("double_weight", double_weight );
                                                                best_params.put("string_weight", string_weight );
                                                                best_params.put("cov_weight", cov_weight );
                                                                best_params.put("div_weight", div_weight );
                                                                best_params.put("unit_weight",unit_weight );
                                                            }
                                                            //hier tussen extra brackets
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                //hier tussen extra brackets
                System.out.println("total combinations were: " + counter);
                long end_time = System.currentTimeMillis();
                long difference = end_time-start_time;
                System.out.println(counter);
                System.out.println(difference/1000 + " seconds.");
                writer.close();
                System.out.println("created train alignments");
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }

        } else { //if testing
            alignments_test.clear();
            alignments2_test.clear();
            //create new alignments using the best params from training set:
            double similarity_threshold = best_params.get("similarity_threshold");
            double min_key_score        = best_params.get("min_key_score");
            double ttest_multiplier     = best_params.get("ttest_multiplier");
            double string_bonus         = best_params.get("string_bonus");
            double max_contained_score  = best_params.get("max_contained_score");

            //weights
            double key_weight           = best_params.get("key_weight");
            double double_weight        = best_params.get("double_weight");
            double string_weight        = best_params.get("string_weight");
            double cov_weight           = best_params.get("cov_weight");
            double div_weight           = best_params.get("div_weight");
            double unit_weight          = best_params.get("unit_weight");


            //initialization:
            boolean assigning;
            List<String> checked_shops = new ArrayList<>();
            double highest_f1=-1;
            Key best_pair_k1;
            Key best_pair_k2;
            Key best_matching_k2;
            double highest_pair_score;
            double highest_k2_score;
            double p_value;
            double double_score;
            double string_score;
            double double_jaccard_score;
            double ttest_score;
            double unit_score;
            double div_score;
            double cov_score;
            double key_score;
            boolean key_contains1;
            boolean key_contains2;
            double is_string;
            int counter=0;
            Double[] f_msm;
            double f1_msm=0;
            double precision_msm=0;
            double recall_msm=0;
            //String w1 = "newegg.com";
           // String w2 = "bestbuy.com";

            //allign keys between webshops
            checked_shops.clear();
            for (String w1 : keys.keySet()){ //loop over webshops
                checked_shops.add(w1);
                for (String w2 : keys.keySet()){
                    if(checked_shops.contains(w2)){continue;} //to prevent double comparisons

            Set<Object> keys1 = new HashSet(keys.get(w1).values());
            Set<Object> keys2 = new HashSet(keys.get(w2).values());

            //test msm
//            f_msm = testmsm(keys1,keys2,0.7);
//            f1_msm = f_msm[0];
//            precision_msm = f_msm[1];
//            recall_msm = f_msm[2];

            assigning = true;
            while(assigning){
                assigning=false;
                best_pair_k1 = null;
                best_pair_k2 = null;
                highest_pair_score=-1;
                for (Object keyobject1 : keys1){
                    Key k1 = (Key) keyobject1; //take one key from webshop 1
                    best_matching_k2 = null;
                    highest_k2_score=-1;

                    for (Object keyobject2 : keys2){
                        Key k2 = (Key) keyobject2; //loop over all keys from webshop 2

                        if (!k1.datatype.equals(k2.datatype))
                            continue; //we dont even score it.

                        p_value = 0;
                        double_score=0;
                        string_score=0;
                        double_jaccard_score=0;
                        ttest_score=0;
                        unit_score=0;
                        div_score=0;
                        cov_score =0;
                        key_score =0;
                        key_contains1=false;
                        key_contains2=false;
                        is_string=0;

                        //key_name
                        key_score = metric.getSimilarity(preProcess(k1.key_name),preProcess(k2.key_name));
                        key_contains1 = preProcess(k1.key_name).contains(preProcess(k2.key_name));
                        key_contains2 = preProcess(k2.key_name).contains(preProcess(k1.key_name));
                        if (key_contains1 | key_contains2){
                            key_score = java.lang.Math.max(key_score,max_contained_score);
                        }
                        if(key_score<min_key_score){continue;} //too risky

                        if (k1.datatype.contains("DOUBLES")) {
                            double[] valuesA = Doubles.toArray(k1.doubleValues);
                            double[] valuesB = Doubles.toArray(k2.doubleValues);

                            if (valuesA.length > 1 && valuesB.length > 1) {
                                //double p_value = km.kolmogorovSmirnovTest(valuesA, valuesB, true);
                                p_value = studentTest.tTest(valuesA, valuesB);
                                ttest_score=p_value;
                            }
                            double_jaccard_score = jaccard_similarity(k1.doubleValues_strings, k2.doubleValues_strings);
                            //System.out.println(ttest_score + "__" + double_jaccard);
                            double_score = java.lang.Math.max(ttest_score,double_jaccard_score);
                            double_score = java.lang.Math.min(double_score,1);


                            if (k1.unit.equals("none") | k2.unit.equals("none")){
                                unit_score = 0;
                            } else {
                                if (k1.unit.equals(k2.unit)) {
                                    unit_score = 1;
                                } else {
                                    unit_score = -1;
                                }
                            }
                        } else { //STRINGS
                            string_score = jaccard_similarity(k1.stringValues, k2.stringValues);
                            is_string = 1;
                        }

                        //coverage
                        cov_score= -java.lang.Math.pow(k1.coverage - k2.coverage, 2.0); //negative squared error
                        //diversity: if one of their diversities is only 1, punish.
                        if (java.lang.Math.min(k1.diversity,k2.diversity)==1){
                            div_score=-1;
                        }

                        double total_score =
                                key_weight      * key_score +
                                        double_weight   * double_score +
                                        string_weight   * string_score +
                                        cov_weight      * cov_score +
                                        div_weight      * div_score +
                                        unit_weight     * unit_score +
                                        is_string       * string_bonus;

                        //which k2 matches this k1 best?
                        if (total_score>highest_k2_score){
                            highest_k2_score=total_score;
                            best_matching_k2=k2;
                        }

                    }

                    //which k1 has the best match?
                    if (highest_k2_score>highest_pair_score){
                        highest_pair_score=highest_k2_score;
                        best_pair_k1 = k1;
                        best_pair_k2 = best_matching_k2;
                    }

                }

                //assign k2 to k1 in 1.

                if (highest_pair_score>=similarity_threshold ){ //has to be higher than some threshold.
                    addMatchingKey(alignments_test,w1, w2, best_pair_k1.key_name, best_pair_k2.key_name);
                    //todo maybe later i should create alignments2 from alignments after this loop.
                    addMatchingKey(alignments2_test, best_pair_k1, best_pair_k2, highest_pair_score);
                    // and remove them from pool.
                    keys1.remove(best_pair_k1);
                    keys2.remove(best_pair_k2);
                    assigning=true;
                }
            }
            }
            }

            Double[] f = hitmatrix_2shops();
            double f1_test = f[0];
            double precision = f[1];
            double recall = f[2];

            try
            {
                FileWriter writer = new FileWriter("./hdfs/nick_phase0_testing.csv",true);
                if (testcount==2){ //2 means the first test round.
                    writer.write("f1_test;precision;recall;f1_msm;precision_msm;recall_msm;f1_train;similarity_threshold;min_key_score;ttest_multiplier;string_bonus;max_contained_score;key_weight;" +
                            "double_weight;string_weight;cov_weight;div_weight;unit_weight");
                    writer.append('\n');
                    writer.flush();
                }


                String str = f1_test+";"+precision+";"+recall+";"+f1_msm+";"+precision_msm+";"+recall_msm+";"+best_params.get("f1_train")+";"+similarity_threshold+";"+min_key_score+";"+ttest_multiplier+";"+string_bonus+";"+max_contained_score+";"+
                        key_weight+";"+double_weight+";"+string_weight+";"+cov_weight+";"+div_weight+";"+unit_weight;
                System.out.println(str);
                writer.write(str);
                writer.append('\n');
                writer.flush();

                writer.close();
                System.out.println("created test alignments");
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            best_params.clear();
        }
    }



    private Double[] hitmatrix_2shops(){
        Map<String, Map> map;
        if (testing==0) {
            map = alignments;
        } else{
            map = alignments_test;
        }
        //create hitmatrix
        int TP=0;
        int FP=0;
        int FN=0;
        int count1=0;
        int count2=0;
        String w1 = "newegg.com";
        String w2 = "bestbuy.com";
        if (!map.containsKey(w1)){
            return new Double[]{0.0, 0.0, 0.0};
        }
        Map<String,Map> temp_object1 = map.get(w1);
        if(!temp_object1.containsKey(w2)){
            return new Double[]{0.0, 0.0, 0.0};
        }
        Map<String,String> temp_object2 = temp_object1.get(w2);
        for (String k1 : temp_object2.keySet()){
            String k2 = temp_object2.get(k1);
            count1++;
            if(NickKeyMatcher.is_aligned(NickKeyMatcher.golden_alignments, w1, w2, k1, k2)){
                TP++;
            }else{
                //    checking(w1,w2,k1,k2);
                FP++;
            }
        }


        if (!NickKeyMatcher.golden_alignments.containsKey(w1)){
            return new Double[]{0.0, 0.0, 0.0};
        }
        Map<String,Map> temp_object1g = NickKeyMatcher.golden_alignments.get(w1);
        if(!temp_object1g.containsKey(w2)){
            return new Double[]{0.0, 0.0, 0.0};
        }
        Map<String,String> temp_object2g = temp_object1g.get(w2);
        for (String k1 : temp_object2g.keySet()){
            count2++;
            String k2 = temp_object2g.get(k1);
            if(!NickKeyMatcher.is_aligned(map, w1, w2, k1, k2)){
//                checking(w1,w2,k1,k2);
                FN++;
            }
        }

        double f1 = (double)(2*TP)/(double)(2*TP + FP + FN);
        double precision = (double)(TP)/(double)(TP + FP);
        double recall = (double)(TP)/(double)(TP + FN);

        return new Double[]{f1, precision, recall};
    }

    public Double[] testmsm(Set<Object> keys1, Set<Object> keys2, double thres){
        Set<StringPair> msm_alignment = new HashSet();
        //InterfaceTokeniser tokeniser = new TokeniserQGram2();
        //AbstractStringMetric metric = new QGramsDistance(tokeniser);
        double key_score;
        for (Object keyobject1 : keys1){
            Key k1 = (Key) keyobject1; //take one key from webshop 1
            for (Object keyobject2 : keys2){
                Key k2 = (Key) keyobject2; //take one key from webshop 1

                key_score = metric.getSimilarity(preProcess(k1.key_name),preProcess(k2.key_name));
                if(key_score>thres){
                    StringPair pair = new StringPair(k1.key_name,k2.key_name);
                    msm_alignment.add(pair);
                }

            }
        }
        System.out.println();
        //create hitmatrix
        int TP=0;
        int FP=0;
        int FN=0;
        String w1 = "newegg.com";
        String w2 = "bestbuy.com";


        for (StringPair pair : msm_alignment){
            String k1 = pair.id_a;
            String k2 = pair.id_b;
            if(NickKeyMatcher.is_aligned(NickKeyMatcher.golden_alignments, w1, w2, k1, k2)){
                TP++;
            }else{
                FP++;
            }
        }


        Map<String,Map> temp_object1g = NickKeyMatcher.golden_alignments.get(w1);
        Map<String,String> temp_object2g = temp_object1g.get(w2);
        for (String k1 : temp_object2g.keySet()){
            String k2 = temp_object2g.get(k1);
            StringPair pair = new StringPair(k1,k2);
            if(!msm_alignment.contains(pair)){
                FN++;
            }
        }
        double f1 = (double)(2*TP)/(double)(2*TP + FP + FN);
        double precision = (double)(TP)/(double)(TP + FP);
        double recall = (double)(TP)/(double)(TP + FN);

        return new Double[]{f1, precision, recall};
    }

    private String findStrings(Matcher strings, String groepen) {
        while (strings.find()) {
            int i = 1;
            groepen = groepen + strings.group(0).toLowerCase();
            i++;
        }
        return groepen;
    }

    private static String findGetallen(Matcher getallen) {
        String groepen;
        groepen = getallen.group(0);

        while (getallen.find()) {
            groepen = groepen + getallen.group(0);
        }
        return groepen;
    }

    private double jaccard_similarity(List<String> x_raw, List<String> y_raw) {
        Set<String> x = new HashSet<String>(x_raw);
        Set<String> y = new HashSet<String>(y_raw);

        int xSize = x.size();
        int ySize = y.size();
        if (xSize == 0 || ySize == 0) {
            return 0.0;
        }

        Set<String> intersectionXY = new HashSet<>(x);
        intersectionXY.retainAll(y);

        return (double) intersectionXY.size() / (double) (xSize < ySize ? xSize : ySize);
    }

    private static String preProcess(String str) {
        str = str.replaceAll("\\, |\\. |\\.$|\\(|\\)", " ");    //remove comma and period at end of word (or period at end of whole string)
        return str.toLowerCase(Locale.ENGLISH);
    }

    public static void addMatchingKey(Map<String,Map> map, String w1, String w2, String k1, String k2){
        //als je een match vind, voeg dan bij alignments.get(w1) een map w2 toe met daarin als <key van w1, key van w2>
        if (!map.containsKey(w1)){
            map.put(w1,new HashMap<String, Map>());
        }
        if(!map.get(w1).containsKey(w2)){
            map.get(w1).put(w2,new HashMap<String, String>() );
        }
        Map<String, String> tempmap = (Map<String, String>) map.get(w1).get(w2);
        tempmap.put(k1,k2);

        //todo not in
        //and the other way around as well:
        if (!map.containsKey(w2)){
            map.put(w2,new HashMap<String, Map>());
        }
        if(!map.get(w2).containsKey(w1)){
            map.get(w2).put(w1,new HashMap<String, String>() );
        }
        Map<String, String> tempmap2 = (Map<String, String>) map.get(w2).get(w1);
        tempmap2.put(k2,k1);
    }

    public static void addMatchingKey(Map<String,Map> map, Key k1, Key k2, double pairscore){
        //als je een match vind, voeg dan bij alignments.get(w1) een map w2 toe met daarin als <key van w1, key van w2>
        String w1 = k1.webshop;
        String w2 = k2.webshop;

        if (!map.containsKey(w1)){
            map.put(w1,new HashMap<String, Map>());
        }
        if(!map.get(w1).containsKey(w2)){
            map.get(w1).put(w2,new HashSet<Keypair>() );
        }
        Set<Keypair> tempmap1 = (Set<Keypair>) map.get(w1).get(w2);

        Keypair keypair1 = new Keypair(k1,k2,k1.datatype,pairscore);
        tempmap1.add(keypair1);

        //and the other way around as well:
        if (!map.containsKey(w2)){
            map.put(w2,new HashMap<String, Map>());
        }
        if(!map.get(w2).containsKey(w1)){
            map.get(w2).put(w1,new HashSet<Keypair>() );
        }
        Set<Keypair> tempmap2 = (Set<Keypair>) map.get(w2).get(w1);

        Keypair keypair2 = new Keypair(k2,k1,k2.datatype,pairscore);
        tempmap2.add(keypair2);
    }



    private static ArrayList<String> extractModelWords(ProductDesc prod, Set<String> keys) {
        ArrayList<String> ret = new ArrayList<>();
        for (String q : keys) {
            String qVal = prod.featuresMap.get(q);
            ret.addAll(extractModelWords(qVal));
        }
        return ret;
    }

    private static Set<String> extractModelWords(String title) {
        Matcher m = MW_PATTERN.matcher(title);
        HashSet<String> c = new HashSet<>();
        while (m.find()) {
            String s = m.group(1);
            c.add(s);
        }
        return c;
    }

    private static Pattern getMWPattern() {
        String regex = "([a-zA-Z0-9]*(([0-9]+[^0-9^,^ ]+)|([^0-9^,^ ]+[0-9]+))[a-zA-Z0-9]*)";
        Pattern p = Pattern.compile(regex);
        return p;
    }

    /**
     * Calculate percentage of matching model words in two sets of model words.
     * Model words are broader here: also purely numeric words are treated as model words.
     */
    private static double mw(ArrayList<String> C, ArrayList<String> D) {
        int totalWords = C.size() + D.size();
        if (totalWords == 0) return 0.0;
        C.retainAll(D);
        D.retainAll(C);    //note: duplicate words in a set is counted twice as match.
        return (C.size() + D.size()) / (double) totalWords;
    }

    public static boolean differentBrands(int p1_id, int p2_id) {
        ProductInfo pInfo1 = productInfoMap.get(p1_id);
        ProductInfo pInfo2 = productInfoMap.get(p2_id);
        if (!pInfo1.titleBrand.equals("unknown") & !pInfo2.titleBrand.equals("unknown")){
            if (!pInfo1.titleBrand.equals(pInfo2.titleBrand)){
                return true; //only if we have both brands and they are unequal, we know for sure that they are different.
            }
        }
        return false;
    }


    public static float productSimilarity(ProductDesc p1, ProductDesc p2) {
//        float duplicates = 0;
//        if (p1.modelID.equals(p2.modelID)){
//            duplicates = 1;
//        }
//        if (p1.hashCode()!=-99999) //always true
//            return duplicates; //perfect similarity
//        if (mu==1){
//            Double[] title_result = newTitleAnalyzer(p1, p2);
//            Double title_score = title_result[0];
//            Double title_count = title_result[1];
//            if (title_count > title_min)
//                return (float) java.lang.Math.min(1,title_score);//max 1
//            else
//                return 0.0f;
//        }

        Map<String, Map> map;
        if (testing==0) {
            map = alignments2;
        } else{
            map = alignments2_test;
        }

        int nr_matches=0;
        double aligned_score=0;
        Set<String> keys1 = new HashSet<>(p1.featuresMap.keySet());
        Set<String> keys2 = new HashSet<>(p2.featuresMap.keySet());

        boolean aligning=true;
        if(!map.containsKey(p1.shop) || !map.get(p1.shop).containsKey(p2.shop)){
            aligning =false; //this webshop combi does not have aligned keys.
        }
        if (aligning){
            //step a: calculate a total score for those features that can be aligned.
            double pair_weight;
            String pair_type;
            double pair_score;
            double total_score =0 ;
            double total_weight =0 ;
            double allowed_diff;
            double d1;
            double d2;

            HashSet<Keypair> keypairs = (HashSet<Keypair>) map.get(p1.shop).get(p2.shop);
            for (Keypair keypair : keypairs){
                if (p1.featuresMap.containsKey(keypair.k1.key_name) && p2.featuresMap.containsKey(keypair.k2.key_name)){
                    String rawvalue1 = p1.featuresMap.get(keypair.k1.key_name);
                    String rawvalue2 = p2.featuresMap.get(keypair.k2.key_name);

                    pair_type = keypair.datatype;
                    if (pair_type.contains("DOUBLES")){

                        d1 = getFirstDouble(keypair.k1,rawvalue1);
                        d2 = getFirstDouble(keypair.k2,rawvalue2);
                        if (d1==-1 || d2==-1){
                            if ((rawvalue1.equals("No") & !rawvalue2.equals("No"))|(rawvalue2.equals("No") & !rawvalue1.equals("No")) )
                                pair_score = 0;
                            else
                                continue; //cannot compare because not both are doubles
                        }else{
                            allowed_diff=java.lang.Math.min(1,0.5*java.lang.Math.min(keypair.k1.std,keypair.k1.std));
                            if (java.lang.Math.abs(d1-d2)<=allowed_diff+0.01)
                                pair_score = 1;
                            else
                                pair_score = 0;
                        }
                    } else {
                        if ((rawvalue1.equals("No") & !rawvalue2.equals("No"))|(rawvalue2.equals("No") & !rawvalue1.equals("No")) )
                            pair_score = 0;
                        else
                            continue; //we dont compare other strings. too unstable.
                    }

                    //calculate weighted score for this key-pair.
                    pair_weight = keypair.pairscore;
                    total_weight += pair_weight;
                    total_score += pair_weight*pair_score;

                    nr_matches ++;
                    keys1.remove(keypair.k1.key_name); //so that they wont be scored again in step b.
                    keys2.remove(keypair.k2.key_name);
                }
            }


            if (nr_matches > 0) {
                aligned_score = total_score / total_weight;
            }
        }

        //step b. calculate a score for the rest ( those features that could not be aligned.)
        ArrayList<String> exMWi = extractModelWords(p1, keys1); //todo remove units? hebben we in onze map keys
        ArrayList<String> exMWj = extractModelWords(p2, keys2);
        double rest_score = mw(exMWi, exMWj);

        double features_score = aligned_score*(1-keys_rest_weight) + rest_score*keys_rest_weight;
        //step c. calculate a score for the similarity of the titles.
        Double[] title_result = newTitleAnalyzer(p1, p2);
        Double title_score = title_result[0];
        Double title_count = title_result[1];


        //step d. weighted average of the scores of steps a,b,c.
        double final_score;

        if (title_count > title_min){
            if(nr_matches>=aligned_min)
                final_score = mu*title_score + (1-mu)*features_score;
            else{
                if (features_score<epsilon)//when it is a bad score, we still want to use it. reasoning: finding an inequality says more than finding an equality.
                    final_score = mu*title_score + (1-mu)*features_score;
                else
                    final_score = title_score;
            }
        }else{
            if(nr_matches>=aligned_min)
                if (title_score<epsilon)//when it is a bad score, we still want to use it. reasoning: finding an inequality says more than finding an equality.
                    final_score = mu*title_score + (1-mu)*features_score;
                else
                    final_score = features_score;
            else
                final_score = 0;
        }

        return (float) java.lang.Math.min(1,final_score);//max 1
    }

    private static Double[] newTitleAnalyzer(ProductDesc p1, ProductDesc p2) {
        double unitscore=0;
        double unitscore_temp=0;
        int unitcount=0;
        double final_score;
        double total_count;

        ProductInfo pInfo1 = productInfoMap.get(p1.ID);
        ProductInfo pInfo2 = productInfoMap.get(p2.ID);
        Map<String,Set<Double>> units1 = pInfo1.titleUnits;
        Map<String,Set<Double>> units2 = pInfo2.titleUnits;
        for (String unit1 :units1.keySet()) {
            for (String unit2 : units2.keySet()) {
                if (unit1.equals(unit2)) { //we found matching units. now we compare their values.
                    Set<Double> values1 = units1.get(unit1);
                    Set<Double> values2 = units2.get(unit2);
                    for (Double value1 : values1) {
                        for (Double value2 : values2) {
                            if (Math.abs(value1 - value2) <= 1.01) {
                                unitscore_temp = 1; //if one of them matches, we are satisfied.
                            }
                        }
                    }
                    unitcount++;
                    unitscore+=unitscore_temp;
                    unitscore_temp=0; //reset
                }
            }
        }
        if (unitcount>0)
            unitscore = unitscore / unitcount;

        double restscore = 0;
        int restcount = 1;
        Set<String> rests1 = pInfo1.titleRest;
        Set<String> rests2 = pInfo2.titleRest;
        if (rests1.isEmpty() | rests2.isEmpty())
            restcount = 0; //we dont count it.
        for (String rest1 :rests1)
            for (String rest2 : rests2)
                if (rest1.contains(rest2) | rest2.contains(rest1) )
                    restscore = 1; //if one of them matches, we are satisfied.


        if (unitcount*restcount>0){ //both are positive, so both have been scored.
            final_score = (1-title_rest_weight)*unitscore + title_rest_weight*restscore; //average
        } else
            final_score = unitscore+restscore; //at least one of them is not scored, so is 0. so range is 0-1.
        total_count = (double)(unitcount+restcount);
        return new Double[]{final_score, total_count};
    }

    public static double getFirstDouble(Key key, String value){
        //everything between brackets is not important:
        if (value.contains("(")){
            value = value.substring(0,value.indexOf("("));
        }
        //more cleaning:
        value = value.replace(",", "")//e.g. in milions. 1,000,000 should be just 1000000
                .replace("x", " ")
                .replace("$", "dollar")
                .replace("\"", "inch")
                .replace("º","degrees")
                .replace("/", " ")
                .replace("”", "inch")
                .replace(":", " ")
                .replace("~", " ")
                .replace("-", " ");

        //strip units:
        for (String unit : key.raw_units){
            value = value.replace(unit,"");
        }
        if (!value.contains("+")){
            //splitting and get double:
            String[] splits = value.split(" +|;");
            for (String waardeSplitted : splits) {
                String foundValue = "";
                Matcher getallen = numberDetector.matcher(waardeSplitted);
                if (getallen.find()) {
                    foundValue = findGetallen(getallen);
                    return Double.parseDouble(foundValue);//we are satisfied with only the first double.
                }
            }
        } else { //in case of multiple values splitted by a +, we take their sum.
            String[] splits = value.split(" +|;");
            double d2=0;
            boolean found=false;
            for (String waardeSplitted : splits) {
                String foundValue = "";
                Matcher getallen = numberDetector.matcher(waardeSplitted);
                if (getallen.find()) {
                    found=true;
                    foundValue = findGetallen(getallen);
                    d2 += Double.parseDouble(foundValue);
                }
            }
            if(found)
                return d2;
        }
        return -1; //means no double could be found.
    }



}
