import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;

import java.io.File;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class EventQueryWork {

    public enum UseCase {PREFIX_SEARCH, FULLTEXT_SEARCH, MLT_SEARCH,EXPERIMENT,PREDICTIONS, RECOMMEND_LIST};
    public static enum AvroEventType {VIEWS,IMPLICIT_VIEWS,QUERYLOGS,USER_ATTRIBUTES}
    public enum ConversionType {CLICK_THROUGH, VIEWS, PURCHASE, RECORD}
    public static int TOTAL_USER;
    public static double TOTAL_SOFTCONVERSION_PERCENT;
    public static double TOTAL_HARDCONVERSION_PERCENT;
    public QAHelper helper;

    public String deviceTypes [] = {"STB","ANDROIDTV","FIRETV","IPAD","IPHONE","WEBPLAYER","iphone","Apple","android"};
    public String profileIds [] = {"mom","dad","son","sister",null}; //conversions are done at userId level -- is this really necessary? probably not, but add it for future impl.
    public String segmentIds [] = {"East","Southeast","NorthEast","West","SouthWest","NorthWest","North"};
    public static JSONObject config = new JSONObject();

    /*don't really care about titles seen by other search type (unless we want to explicitly support cross use-case soft conversion -- in that case we should use the shared map from the calling object QAHelper
    but doing so will be costly and might have unpredictable results since we might have up to 12 threads trying to read and update to the map at once - so going with the easy path - a particular use case should only
    care about its own soft and hard conversion history)
     */
    static private volatile HashMap <String, Set> generic_seen_by_history = new HashMap<>();
    static private volatile HashMap <String, Set> generic_hard_conversion_history = new HashMap<>();
    static private volatile HashMap <String, Integer> failed_searches = new HashMap<>();
    static private volatile HashMap <String, Set<String>> userDeviceIds = new HashMap<>();
    static private volatile Set<String> uniqueDeviceIdSet = new HashSet<>();
    static private volatile Set<String> uniqueProfileIdSet = new HashSet<>();
    static private volatile HashMap <String,Integer> userProfileIds = new HashMap<>();

    static private List viewEventEndpoint, purchaseEventEndpoint,recordingEventEndpoint,clickEventEndpoint;
    private static List prefix_urls,fulltext_urls,mlt_urls,experiment_urls,predictions_urls;
    private static String debugFFS="&__debugffs=querylog"; //let's just default to querylog since we can get rule activation even if we're not specifically running experiment

    /* when set to true, this allows us to pick a random user in the past that has a search history and convert soft against those past search results
     * if set to false - then convert directly on the current search result -- simply by omitting a queryId will turn the conversion into a soft type
     */
    private static boolean allowCrossTypeSoftConversion = false;
    private static boolean enableExperiments = false; // turn on to generate data and conversion for an experiment or campaign
    private static boolean postStationId = false; // should we attempt to find a stationId for the given item? could slow things down
    private static String userPrefix [] = new String[] {}; // we might want to generate against different set of users in across tests, or the same set of users
    private static String [] experimentTitleIds_ToConvert = new String[]{};
    private static boolean alwaysConvertOnExperimentPromotionId = false; // when set to true always convert the top id that is being promoted, otherwise choose any random id from result to convert
    private static String[] searchTermOverrides = new String []{};
    private static boolean mixSearchTermWithRandomCoreTitles = false;
    private static String[] searchIds = new String[]{};

    /* setup experiment if configuration includes experiment test */
    public void setupExperiment(){
       enableExperiments = config.getBoolean("enableExperiments");
        if(enableExperiments) {
            try {
                experiment_urls=config.getJSONObject("useCaseTaps").getJSONArray("EXPERIMENTS_TAP").toList();
                enableExperiments = config.getBoolean("enableExperiments");
                experimentTitleIds_ToConvert = (String[]) config.getJSONArray("experimentTitleIds_ToConvert").toList().toArray(experimentTitleIds_ToConvert);
                alwaysConvertOnExperimentPromotionId = config.getBoolean("alwaysConvertOnExperimentPromotionId");
                JSONObject overrides = config.getJSONObject("searchOverrides");
                searchTermOverrides =overrides.getJSONArray("searchTerms").toList().toArray(searchTermOverrides);
                searchIds = overrides.getJSONArray("searchIds").toList().toArray(searchIds);
                mixSearchTermWithRandomCoreTitles = overrides.getBoolean("mixSearchTermWithRandomCoreTitles");
            } catch (Exception e){
                System.out.printf("Not all experiment properties are set"+ e); //warn -- we may be using defaults i.e no search term overrides
            }
        }
    }


    /*
    Separate out the query generation into a separate class so that events spawning viewing, purchases, recordings can be run concurrently
    @Param UseCase - whether this is from Prefix_Search, FullText_Search, or MLT_Search
    @Param QAHelper - the main calling class, it has plenty of functions we need and some shared data our threads will require
    @Param JSONBObject - the config json that tells how many queries to generate for each taps/use_case and the number of conversion for the each type -i.e views,clicks broken down further by soft vs hard
     */
    public EventQueryWork(UseCase eventType, QAHelper qaHelper, JSONObject config) throws Exception {
        this.config = config;
        this.TOTAL_USER = config.getInt("totalUsers");
        int totalEvents = config.getInt("totalEvents"); // the total queries to generate across different types of paths
        searchTermOverrides =config.getJSONObject("searchOverrides").getJSONArray("searchTerms").toList().toArray(searchTermOverrides);

        double TOTAL_VIEW_CONVERSION_PERCENT = config.getJSONObject("totalConversionCountsByConversionType_percent").getDouble("VIEW");
        double TOTAL_CLICK_THROUGH_CONVERSION_PERCENT = config.getJSONObject("totalConversionCountsByConversionType_percent").getDouble("CLICK_THROUGH");
        double TOTAL_PURCHASE_CONVERSION_PERCENT = config.getJSONObject("totalConversionCountsByConversionType_percent").getDouble("PURCHASE");
        double TOTAL_RECORDING_CONVERSION_PERCENT = config.getJSONObject("totalConversionCountsByConversionType_percent").getDouble("RECORD");

        this.TOTAL_HARDCONVERSION_PERCENT = config.getJSONObject("hardSoftConversion_split_percent").getDouble("hard");
        this.TOTAL_SOFTCONVERSION_PERCENT = config.getJSONObject("hardSoftConversion_split_percent").getDouble("soft");

        double TOTAL_PREFIX_USECASE_CONVERSION_PERCENT = config.getJSONObject("totalConversionByUseCase_percent").getDouble("PREFIX_SEARCHES");
        double TOTAL_FULLTEXT_USECASE_CONVERSION_PERCENT = config.getJSONObject("totalConversionByUseCase_percent").getDouble("FULLTEXT_SEARCHES");
        double TOTAL_MLT_USECASE_CONVERSION_PERCENT = config.getJSONObject("totalConversionByUseCase_percent").getDouble("MLT_SEARCHES");
        double TOTAL_PREDICTIONS_USECASE_CONVERSION_PERCENT = config.getJSONObject("totalConversionByUseCase_percent").getDouble("PREDICTION_SEARCHES");

        allowCrossTypeSoftConversion = config.getBoolean("allowCrossTypeConversions");
        prefix_urls = config.getJSONObject("useCaseTaps").getJSONArray("PREFIX_SEARCHES").toList();
        fulltext_urls = config.getJSONObject("useCaseTaps").getJSONArray("FULLTEXT_SEARCHES").toList();
        mlt_urls = config.getJSONObject("useCaseTaps").getJSONArray("MLT_SEARCHES").toList();
        predictions_urls =  config.getJSONObject("useCaseTaps").getJSONArray("PREDICTION_SEARCHES").toList();

        clickEventEndpoint = config.getJSONObject("eventPostEndPoints").getJSONArray("clickEvents").toList();
        viewEventEndpoint = config.getJSONObject("eventPostEndPoints").getJSONArray("viewEvents").toList();
        purchaseEventEndpoint = config.getJSONObject("eventPostEndPoints").getJSONArray("purchaseEvents").toList();
        recordingEventEndpoint =  config.getJSONObject("eventPostEndPoints").getJSONArray("recordingEvents").toList();
        this.postStationId = config.getBoolean("postStationId");
        setupExperiment(); // if there are experiments to be run as part of this config file, set it up

        this.userPrefix = (String []) config.getJSONArray("userPrefix").toList().toArray(this.userPrefix);




        this.helper = qaHelper;
        switch (eventType) {
            case PREFIX_SEARCH:
                int totalPrefixEvents = (int) (TOTAL_PREFIX_USECASE_CONVERSION_PERCENT*totalEvents);
                doQueryAndConversionWork(totalPrefixEvents, TOTAL_VIEW_CONVERSION_PERCENT, TOTAL_CLICK_THROUGH_CONVERSION_PERCENT, TOTAL_PURCHASE_CONVERSION_PERCENT, TOTAL_RECORDING_CONVERSION_PERCENT, prefix_urls,UseCase.PREFIX_SEARCH);
                break;

            case FULLTEXT_SEARCH:
                int totalFullTextEvents = (int) (TOTAL_FULLTEXT_USECASE_CONVERSION_PERCENT*totalEvents);
                doQueryAndConversionWork(totalFullTextEvents , TOTAL_VIEW_CONVERSION_PERCENT, TOTAL_CLICK_THROUGH_CONVERSION_PERCENT, TOTAL_PURCHASE_CONVERSION_PERCENT, TOTAL_RECORDING_CONVERSION_PERCENT, fulltext_urls,UseCase.FULLTEXT_SEARCH);
                break;

            case MLT_SEARCH:
                int totalMLTEvents = (int) (TOTAL_MLT_USECASE_CONVERSION_PERCENT*totalEvents);
                doQueryAndConversionWork(totalMLTEvents, TOTAL_VIEW_CONVERSION_PERCENT, TOTAL_CLICK_THROUGH_CONVERSION_PERCENT, TOTAL_PURCHASE_CONVERSION_PERCENT, TOTAL_RECORDING_CONVERSION_PERCENT, mlt_urls,UseCase.MLT_SEARCH);
                break;

            case EXPERIMENT:
                int totalExperimentConversionGoal = (int) (1.0*totalEvents);
                doQueryAndConversionWork(totalExperimentConversionGoal, TOTAL_VIEW_CONVERSION_PERCENT, TOTAL_CLICK_THROUGH_CONVERSION_PERCENT, TOTAL_PURCHASE_CONVERSION_PERCENT, TOTAL_RECORDING_CONVERSION_PERCENT, experiment_urls,UseCase.EXPERIMENT);
                break;

            case PREDICTIONS:
                int totalPredictionEvents = (int) (TOTAL_PREDICTIONS_USECASE_CONVERSION_PERCENT*totalEvents);
                doQueryAndConversionWork(totalPredictionEvents, TOTAL_VIEW_CONVERSION_PERCENT, TOTAL_CLICK_THROUGH_CONVERSION_PERCENT, TOTAL_PURCHASE_CONVERSION_PERCENT, TOTAL_RECORDING_CONVERSION_PERCENT, predictions_urls,UseCase.PREDICTIONS);
                break;
        }

    }

    public EventQueryWork(QAHelper helper, JSONObject config){
        this.config = config;
        this.postStationId = config.getBoolean("postStationId");
        setupExperiment();
        this.helper = helper;
    }

    /* generic event converter method -- pass in the total events you want to convert, and what percent should be hard and soft conversion
     @Param  totalEvents - the total event count for a particular use case
     @Param conversionType - CLICK_THROUGH, VIEWS, RECORD, PURCHASE
     */
    public void convertEvents(int totalEvents, List searchURLS, List eventEndpoints, UseCase useCaseType, ConversionType conversionType) {
        System.out.printf("Running %s for %s with totalEvents %d\n",useCaseType,conversionType,totalEvents);
        int softCounts=0; int hardCounts=0;
        int lookAheadMin = 1; int lookAheadMax = 61; // randomize lookAhead time between 0 minutes an 60 minutes
        boolean  enableProfileLevel=false;
        try{
            lookAheadMax = getLookAheadTime(useCaseType,conversionType,this.config,LookAheadType.MAX);
            lookAheadMin = getLookAheadTime(useCaseType,conversionType,this.config,LookAheadType.MIN);
            enableProfileLevel = config.getBoolean("enableProfileLevel");
        } catch (Exception e){
        }

        String profileId= null;

        for (int i = 0; i < totalEvents; i++) {
            String deviceType = deviceTypes[new Random().nextInt(deviceTypes.length)];
            String segment = segmentIds[new Random().nextInt(segmentIds.length)];
            String randomUser = userPrefix[0];
            if(enableProfileLevel){
                profileId = profileIds[new Random().nextInt(profileIds.length)]+UUID.randomUUID().toString();
            }
            if(userPrefix.length>1){
                randomUser = userPrefix[new Random().nextInt(userPrefix.length)];
            }
            randomUser += (new Random().nextBoolean())?new Random().nextInt(TOTAL_USER):""; // generate a random user between 0 and totalUsers - 1

            String searchURL = searchURLS.get(new Random().nextInt(searchURLS.size())).toString(); // randomize between our flex nodes and admin -- simulate load balancer
            String url = searchURL;
            long NOW = System.currentTimeMillis();
            long VIEW_NOW = System.currentTimeMillis();
            //if now is set in config to do past X Days, then use that
            try{
                VIEW_NOW=NOW-TimeUnit.DAYS.toMillis(config.getInt("eventFromPastDays"));
            } catch (Exception e){}


            String deviceId = "dev"+new Random().nextInt(2)+UUID.randomUUID().toString();
            String searchTerm="";
            try {
                switch (useCaseType){
                    case MLT_SEARCH:
                        searchTerm = getRandomSearchId();
                        url = url + searchTerm + "&__deviceType=" + deviceType + "&__userId=" + randomUser+"&__NOW="+NOW+"&__segment="+segment+debugFFS+"&profileId="+profileId+"&deviceId="+deviceId;
                        break;
                    case PREFIX_SEARCH:
                        searchTerm = getRandomPrefixSearchTerm();
                        url = url + searchTerm + "&__deviceType=" + deviceType + "&__userId=" + randomUser+"&__NOW="+NOW+"&__segment="+segment+debugFFS+"&profileId="+profileId+"&deviceId="+deviceId;
                        break;
                    case FULLTEXT_SEARCH:
                        searchTerm = helper.hexEncodedTitle(getRandomSearchTerm());
                        url = url + searchTerm + "&__deviceType=" + deviceType + "&__userId=" + randomUser+"&__NOW="+NOW+"&__segment="+segment+debugFFS+"&profileId="+profileId+"&deviceId="+deviceId+"&__group=true&__group.by=releaseYear";
                        break;
                    case RECOMMEND_LIST:
                        searchTerm = helper.hexEncodedTitle(getRandomSearchTerm());
                        url = url + searchTerm + "&__deviceType=" + deviceType + "&__userId=" + randomUser+"&__NOW="+NOW+"&__segment="+segment+debugFFS+"&profileId="+profileId+"&deviceId="+deviceId;
                        break;
                    case EXPERIMENT:
                        searchTerm = helper.hexEncodedTitle(getRandomSearchTerm());
                        url = url + searchTerm + "&deviceType=" + deviceType + "&userId=" + randomUser+"&__NOW="+NOW+"&__segment="+segment+debugFFS+"&profileId="+profileId+"&deviceId="+deviceId;
                        break;
                    case PREDICTIONS:
                        searchTerm = helper.hexEncodedTitle(getRandomSearchTerm());
                        url = url + searchTerm + "&deviceType=" + deviceType + "&userId=" + randomUser+"&__NOW="+NOW+"&__segment="+segment+debugFFS+"&profileId="+profileId+"&deviceId="+deviceId;
                        break;
                }

                String activatedGroup =null;

                //(1) Send the case specific query event
                System.out.printf("%s query event for %s conversion to %s \n", useCaseType,conversionType, url);
                JSONObject results = new JSONObject(helper.sendGetEventToEndPoint(url));
                activatedGroup = getActivatedExperimentVariationName(results);

                String softConversionKey = activatedGroup==null?"":activatedGroup+"_"+conversionType+"_HARD_CONVERSION";
                String hardConversionKey = activatedGroup==null?"":activatedGroup+"_"+conversionType+"_SOFT_CONVERSION";
                
                String resultType = "none";
                resultType = results.keySet().contains("hits") ? "hits" : results.keySet().contains("terms") ? "terms" : results.keySet().contains("groups") ? "groups" : results.keySet().contains("item") ? "item" : results.keySet().contains("facetCounts") ? "facetCounts" : results.keySet().contains("lists") ? "lists":"data";

                //get the timestamp reported back by the server as response to this query event, this is the true timestamp on querylog topic being used by etls
                NOW = results.getJSONObject("debug").getJSONObject("queryEvent").getLong("timestamp");
                try {
                    // (2) store first 10 hits for the user in a shared has_seen volatile for history
                    String [] hitIds = helper.collectHitIds(results.getJSONArray(resultType),resultType);
                    int randTopHitIndex = new Random().nextInt(hitIds.length); //retrieve a random item from the top 10
                    String idToConvert = hitIds[randTopHitIndex]; //set the item as the id to convert

                    /*
                    Keep history of items user have seen if cross-conversion type is allowed. Meaning if the user have seen this item from a click through query event, we can use it
                    and  do a soft conversion here for another event such as purchase, recording, or views
                    ...do this only for non-experiments.
                     */
                    if(allowCrossTypeSoftConversion && !useCaseType.equals(UseCase.EXPERIMENT)){
                        if(generic_seen_by_history.get(randomUser)==null){
                            generic_seen_by_history.put(randomUser,new HashSet(Arrays.asList(hitIds)));
                        } else {
                            Set seen = generic_seen_by_history.get(randomUser); //retrieve the current user's past seen items
                            seen.addAll(Arrays.asList(hitIds)); //update current top item to be part of the seen items
                            generic_seen_by_history.put(randomUser,seen);
                        }

                        if(new Random().nextBoolean()) {
                            //pick a random user from the past to do soft conversion
                            randomUser = (String) generic_seen_by_history.keySet().toArray()[new Random().nextInt(generic_seen_by_history.size())];
                            // get random item from the random user seen list
                            idToConvert = (String) generic_seen_by_history.get(randomUser).toArray()[new Random().nextInt(generic_seen_by_history.get(randomUser).size())];
                        }
                    }

                    NOW = NOW +TimeUnit.SECONDS.toMillis(new Random().nextInt(5)+1); //update NOW to be more realistic,a brief time (seconds) passed after the user received the results to their actual interaction
                    VIEW_NOW = VIEW_NOW+TimeUnit.SECONDS.toMillis(new Random().nextInt(5)+1); // for doing past views to test time affinity
                    //long endTime = NOW + TimeUnit.MINUTES.toMillis(new Random().ints(lookAheadMin,lookAheadMax).findAny().getAsInt()); // defaults to 0 - 60 minutes if no config for lookAhead
                    long endTime = VIEW_NOW + TimeUnit.MINUTES.toMillis(new Random().ints(lookAheadMin,lookAheadMax).findAny().getAsInt()); // defaults to 0 - 60 minutes if no config for lookAhead

                    JSONObject data = new JSONObject();
                    if(conversionType.equals(ConversionType.VIEWS)){
                        //
                        data = new JSONObject(helper.generateViewEventData(randomUser,idToConvert,VIEW_NOW,endTime+TimeUnit.MINUTES.toMillis(new Random().ints(30,60).findFirst().getAsInt())));
                        data.put("deviceType",deviceType);
                        data.put("segment",segment);
                        data.put("profileId",profileId); // if it's a view, set the profileId (null) if profileLevel is not enabled
                        //there is no deviceId, add to it
                        data.put("deviceId",deviceId);
                        data.put("timestamp",endTime); //timestamp of a view should start it at x lookAhead minutes after results were returned
                        data = setStationId(data,idToConvert);
                    } else {
                        // an implicit events needs only timestamp of when the action occurred, so we set the action as the endTime
                        data = new JSONObject(helper.generateImplicitViewEvents(randomUser,idToConvert,endTime,deviceType));
                        data.put("deviceType",deviceType);
                        data.put("segment",segment);
                        //data.put("profileId",profileId); implicit events aren't part of views so they should not affect audience measurement, so let's ignore this and deviceId for now
                        //generateImplicit events already defaults to ITEM_DETAILS (for CLICK_THROUGH), everything else, we set it by their conversionType name
                        if(!conversionType.equals(ConversionType.CLICK_THROUGH)){
                            data.put("type",conversionType);
                        }
                    }

                    //don't bother breaking down hard/soft conversion if it's an experiment --  our goal should be to convert 100% of the time, for simplicity this will be both soft & hard
                    if(useCaseType.equals(UseCase.EXPERIMENT)){
                        if(!enableExperiments) {System.out.println("The configuration does not support experiment -- please enable the property and try again"); continue;}

                        //setup hard conversion for experiment
                        data.put("queryId",results.getString("queryId"));

                        // we should convert only if there is an impression -- if there is no impression -- don't convert
                        //convert only if there was an impression/activation that happened
                        if(activatedGroup!=null){
                            //set impression and convert
                            setVariationGroupImpressions(activatedGroup,conversionType,randomUser); // 100% conversion -- convert on every impression
                            if(alwaysConvertOnExperimentPromotionId) idToConvert = hitIds[0]; else idToConvert = hitIds[randTopHitIndex]; // Pick an id to do the conversions

                            //always convert on the target group
                            if(activatedGroup.contains("target")){
                                data.put("itemId",idToConvert); //make sure a promoted target id always get converted (not necessary, but makes verifying results easier)
                                //convert hard (JSONObject data, String endPointURL, String conversion,ConversionType conversionType, String conversionKey)
                                convertOnExperimentImpression(data,eventEndpoints.get(new Random().nextInt(eventEndpoints.size())).toString(),"hard",conversionType,hardConversionKey,activatedGroup);
                                //convert soft
                                convertOnExperimentImpression(data,eventEndpoints.get(new Random().nextInt(eventEndpoints.size())).toString(),"soft",conversionType,softConversionKey,activatedGroup);
                            } else {
                                boolean convert = new Random().nextBoolean();
                                if(convert){
                                    //convert hard
                                    convertOnExperimentImpression(data,eventEndpoints.get(new Random().nextInt(eventEndpoints.size())).toString(),"hard",conversionType,hardConversionKey,activatedGroup);
                                    //convert soft
                                    convertOnExperimentImpression(data,eventEndpoints.get(new Random().nextInt(eventEndpoints.size())).toString(),"soft",conversionType,softConversionKey,activatedGroup);
                                }
                            }
                        }
                        continue; //the rest of the logic below are for non-experiments, so skip them
                    }


                    String useCase_softConversionKey = conversionType+"_SOFT_CONVERSION";
                    String useCase_hardConversionKey = conversionType+"_HARD_CONVERSION";


                    // let's simplify things by sending same amount of hard and soft conversion
                    //(3a) add in queryId to send in the POST event for the hardConversion
                    data.put("queryId",results.getString("queryId"));
                    boolean alreadyConvertedByActivatedExperiment = false;
                    boolean alreadyConvertedByActivatedExperiment_Soft = false;
                    //this might not be an experiment run, but its possible an experiment was activated regardless because of those random titles
                    if(activatedGroup!=null){
                        setVariationGroupImpressions(getActivatedExperimentVariationName(results),conversionType,randomUser); // set impression only once -- no need to set at soft impression as well
                        alreadyConvertedByActivatedExperiment = convertOnExperimentImpression(data,eventEndpoints.get(new Random().nextInt(eventEndpoints.size())).toString(),"hard",conversionType,softConversionKey);
                        if(alreadyConvertedByActivatedExperiment) updateUseCaseConversionCount(useCase_hardConversionKey); // we will not attempt to convert again if its converted by an impression
                    }
                    else {
                        //do our normal useCase conversion because there was no interruption by a activated group
                        if(helper.postEventToEndpoint("["+data.toString()+"]",eventEndpoints.get(new Random().nextInt(eventEndpoints.size())).toString()).contains("202")) {
                            hardCounts++;
                        /*we might not have converted on this user before -- simply add the new id to the set
                        for the current useCase (i.e fulltext) conversionType (i.e clicks) the generic_hard_conversion_history map holds
                        the list of unique users and their converted items (if the user converted on the same item twice -- we will not get accurate total conversion count
                        and this is more likely to happen when we allow crossType conversions
                         */
                            if(generic_hard_conversion_history.get(randomUser)==null){
                                Set converted = new HashSet();
                                converted.add(idToConvert);
                                generic_hard_conversion_history.put(randomUser,converted);
                                // since this user already have a hard conversion -- append the new itemId to its conversion history
                            } else {
                                Set converted = generic_hard_conversion_history.get(randomUser);
                                converted.add(idToConvert);
                                generic_hard_conversion_history.put(randomUser,converted);
                            }
                            updateUseCaseConversionCount(useCase_hardConversionKey);
                        }

                        // track a specific user's deviceId so we can count overall device count for views
                        if(conversionType.equals(ConversionType.VIEWS)){
                            uniqueProfileIdSet.add(profileId);
                            if(userDeviceIds.get(randomUser)==null){
                                Set <String> dev = new HashSet<>();
                                dev.add(deviceId);
                                userDeviceIds.put(randomUser,dev);
                                uniqueDeviceIdSet.add(deviceId);
                            } else {
                                //add just at most 2 deviceId per user
                                Set <String> dev = new HashSet<>();
                                dev = userDeviceIds.get(randomUser);
                                if(dev.size()<2){
                                        dev.add(deviceId);
                                        userDeviceIds.put(randomUser,dev);
                                        uniqueDeviceIdSet.add(deviceId);
                                }
                            }
                        }

                    }



                    //now do soft conversion
                    if(activatedGroup!=null){
                        alreadyConvertedByActivatedExperiment_Soft = convertOnExperimentImpression(data,eventEndpoints.get(new Random().nextInt(eventEndpoints.size())).toString(),"soft",conversionType,softConversionKey);
                        if(alreadyConvertedByActivatedExperiment_Soft) {
                            updateUseCaseConversionCount(useCase_hardConversionKey); // we will not attempt to convert again if its converted by an impression
                            softCounts++;
                        }
                    }
                    //it was not pre-empted by an experiment activation for this random search so no soft conversion was converted by that interruption -- resume our normal use case conversion for soft conversion
                    else try {
                        randTopHitIndex = (randTopHitIndex-1)<0?0:(randTopHitIndex-1); // simulate the user interacting with another one of the top 10 results (use the same item if there is only 1)
                        String itemId = hitIds[randTopHitIndex];
                        data.put("queryId",JSONObject.NULL); // there should be no queryId in a soft conversion
                        data.put("itemId",itemId);

                        NOW = NOW +TimeUnit.SECONDS.toMillis(new Random().nextInt(5)+1); // let the second view as a soft conversion a few seconds after the hard conversion
                        endTime = NOW + TimeUnit.MINUTES.toMillis(new Random().ints(lookAheadMin,lookAheadMax).findAny().getAsInt()); // defaults to 0 - 60 minutes if no config for lookAhead
                        //update the start and end time for view events

                        if(conversionType.equals(ConversionType.VIEWS)) {
                            data.put("startTime",endTime);
                            data.put("timestamp",endTime);
                            data.put("endTime",endTime+TimeUnit.MINUTES.toMillis(new Random().ints(30,60).findFirst().getAsInt()));
                            data = setStationId(data,idToConvert);
                        } else {
                            //NOW should still be based on lookAheadTime, and it should based on our lookAhead interval
                            NOW = NOW+TimeUnit.MINUTES.toMillis(new Random().ints(lookAheadMin,lookAheadMax).findFirst().getAsInt());
                            data.put("timestamp",NOW); // if it's not a view, update only the timestamp
                        }

                        //send the post event for click soft conversion
                        if(helper.postEventToEndpoint("["+data.toString()+"]",eventEndpoints.get(new Random().nextInt(eventEndpoints.size())).toString()).contains("202")) {
                            softCounts++;
                            updateUseCaseConversionCount(useCase_softConversionKey);
                            // track a specific user's deviceId so we can count overall device count for views
                            if(conversionType.equals(ConversionType.VIEWS)){
                                uniqueProfileIdSet.add(profileId);
                                if(userDeviceIds.get(randomUser)==null){
                                    Set <String> dev = new HashSet<>();
                                    dev.add(deviceId);
                                    userDeviceIds.put(randomUser,dev);
                                    uniqueDeviceIdSet.add(deviceId);
                                } else {
                                    //add just at most 2 deviceId per user
                                    Set <String> dev = new HashSet<>();
                                    dev = userDeviceIds.get(randomUser);
                                    if(dev.size()<2){
                                            dev.add(deviceId);
                                            userDeviceIds.put(randomUser,dev);
                                            uniqueDeviceIdSet.add(deviceId);
                                    }
                                }
                            }
                        }

                    } catch (Exception e){
                        System.out.println("Ran into a snag with soft conversion "+e +" The total hits was "+hitIds.length+" we tried to send in "+Math.abs(randTopHitIndex-1));
                    }


                } catch (Exception e){
                    //this would happen if we get zero results and the call to collect hits throw an out of bound exception since its passes zero to Random nextInt
                    if(e instanceof IllegalArgumentException){
                        if(failed_searches.get(searchTerm)==null){
                            failed_searches.put(searchTerm,1);
                        } else failed_searches.put(searchTerm,failed_searches.get(searchTerm)+1);
                    }
                }


            } catch (Exception e) {
                //fail to get random title
            }
        }
    }

    /*
    Update the experiment_conversionCounts Map
     */
    private boolean convertOnExperimentImpression(JSONObject data, String endPointURL, String conversion,ConversionType conversionType, String conversionKey,String... activatedGroupDebug) throws Exception {
        if(conversion.contains("soft")){
            long NOW = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1); // about 1 hour after the hard conversion, a soft conversion is followed through
            long endTime = NOW+TimeUnit.MINUTES.toMillis(new Random().longs(30,60).findAny().getAsLong());
            data.put("queryId",JSONObject.NULL);
            if(!conversionType.equals(ConversionType.VIEWS)){
                data.put("timestamp",NOW);
            } else {
                data.put("startTime",NOW);
                data.put("endTime",endTime);
                data.put("endOffset",TimeUnit.MILLISECONDS.toSeconds(endTime - NOW));
            }
        }

        if(activatedGroupDebug.length==1){
            String aG = activatedGroupDebug[0];
            if(aG.contains("target") && conversionType.equals(ConversionType.VIEWS)){
                    try {
                        Assert.assertTrue(data.get("itemId")+" does not equal to id to convert at index 0 "+experimentTitleIds_ToConvert[0],  data.getString("itemId").equalsIgnoreCase(experimentTitleIds_ToConvert[0]));
                    } catch (AssertionError assertionError){
                        //our configuration allows to convert non-promoted id
                        System.out.println(assertionError.getMessage());
                    }
            }
        }

        if(helper.postEventToEndpoint("["+data.toString()+"]",endPointURL).contains("202")){
            if(experiment_conversionCounts.get(conversionKey)!=null){
                experiment_conversionCounts.put(conversionKey,experiment_conversionCounts.get(conversionKey)+1);
            } else experiment_conversionCounts.put(conversionKey,1);
            return true;
        } return false;
    }

    /*
    Update the useCase_conversionCount Map
     */
    public void updateUseCaseConversionCount(String conversionKey) throws Exception{
        if(useCase_conversionCounts.get(conversionKey)!=null){
            useCase_conversionCounts.put(conversionKey,useCase_conversionCounts.get(conversionKey)+1);
        } else useCase_conversionCounts.put(conversionKey,1);
    }


    /*
    Do the actual events generating/querying and conversion actions here based on configuration passed in from the config file
     */
    public void doQueryAndConversionWork(int totalQueries, double viewPercent, double clickPercent, double purchasePercent, double recordingPercent, List searchURLS, UseCase useCaseType) throws Exception {
        final int totalViewEvents = (int) Math.round(totalQueries * viewPercent);
        final int totalClickEvents = (int) Math.round(totalQueries * clickPercent);
        final int totalPurchaseEvents = (int) Math.round(totalQueries * purchasePercent);
        final int totalRecordingEvents = (int) Math.round(totalQueries * recordingPercent);

        System.out.printf("Use case %s total Queries %d = sum of all queryEventTypes %d \n", useCaseType,totalQueries, (totalClickEvents + totalViewEvents + totalPurchaseEvents + totalRecordingEvents));
        System.out.println("Views queries & conversion " + totalViewEvents);
        System.out.println("ClickEvents queries & conversion " + totalClickEvents);
        System.out.println("PurchaseEvents queries & conversion " + totalPurchaseEvents);
        System.out.println("Recording queries & conversion " + totalRecordingEvents);

        long startTime = System.currentTimeMillis();

        Thread eventsForViews = new Thread(){
            public void run(){
                convertEvents(totalViewEvents,searchURLS,viewEventEndpoint,useCaseType,ConversionType.VIEWS);
            }
        };

        Thread eventsForClicks = new Thread() {
            public void run() {
                convertEvents(totalClickEvents,searchURLS,clickEventEndpoint,useCaseType,ConversionType.CLICK_THROUGH);
            }
        };

        Thread eventsForPurchase = new Thread(){
            public void run(){
                convertEvents(totalPurchaseEvents,searchURLS,purchaseEventEndpoint,useCaseType,ConversionType.PURCHASE);
            }
        };

        Thread eventsForRecording = new Thread() {
            public void run() {
                convertEvents(totalRecordingEvents,searchURLS,recordingEventEndpoint,useCaseType,ConversionType.RECORD);
            }
        };

        eventsForViews.start();
        eventsForClicks.start();
        eventsForPurchase.start();
        eventsForRecording.start();

        eventsForClicks.join();
        eventsForViews.join();
        eventsForPurchase.join();
        eventsForRecording.join();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        String summary = String.format("\n"+useCaseType +":Processed %d queries took %d hours %d minutes %d seconds\n",totalQueries,TimeUnit.MILLISECONDS.toHours(duration),TimeUnit.MILLISECONDS.toMinutes(duration),TimeUnit.MILLISECONDS.toSeconds(duration));
        summary = summary.concat(useCaseType+":Total successful conversions: "+getUniqueConversionsByAllUsers());
        summary = summary.concat("\n"+useCaseType+" Failed Searches: "+failed_searches);
        System.out.println(summary);
        helper.use_case_conversion_summary.append(summary);
        helper.use_case_conversion_summary.append("\n"+useCaseType+" - Generic Campaign Summary: \n"+helper.prettyPrintMap(useCase_conversionCounts)+"\n");
        //threads are done working -- we can update the main shared map
        if(useCaseType.equals(UseCase.MLT_SEARCH)){
            helper.mlt_hard_conversions = generic_hard_conversion_history;
        } else if(useCaseType.equals(UseCase.FULLTEXT_SEARCH)){
            helper.ft_hard_conversions = generic_hard_conversion_history;
        } else if(useCaseType.equals(UseCase.PREFIX_SEARCH)){
            helper.prefix_hard_conversions = generic_hard_conversion_history;
        } else if(useCaseType.equals(UseCase.PREDICTIONS)){
            helper.predictions_hard_conversions = generic_hard_conversion_history;
        }
        else {
            helper.experiment_conversions=experiment_conversionCounts;
        }
        int totalDevices = 0;
        for(String name: userDeviceIds.keySet()){
            totalDevices+=userDeviceIds.get(name).size();
        }
        System.out.println("total devices for all users are "+totalDevices);
    }

    /*
    Given the conversion data we generated from our property file, generate a CSV file that details our expected conversion break down so we can use to compare it
    against the ETL generated conversion within the Use Case Performance Overview Dashboard inside Product module of Insight
     */
    public void generateExpectedConversionReportFromTest(long startTime, long endTime, boolean...experimentRan) throws Exception {
        double views_conversion_rate = config.getJSONObject("totalConversionCountsByConversionType_percent").getDouble("VIEW");
        double clicks_conversion_rate = config.getJSONObject("totalConversionCountsByConversionType_percent").getDouble("CLICK_THROUGH");
        double purchase_conversion_rate = config.getJSONObject("totalConversionCountsByConversionType_percent").getDouble("PURCHASE");
        double record_conversion_rate = config.getJSONObject("totalConversionCountsByConversionType_percent").getDouble("RECORD");

        double prefix_conversion_rate = config.getJSONObject("totalConversionByUseCase_percent").getDouble("PREFIX_SEARCHES");
        double fulltext_conversion_rate = config.getJSONObject("totalConversionByUseCase_percent").getDouble("FULLTEXT_SEARCHES");
        double mlt_conversion_rate = config.getJSONObject("totalConversionByUseCase_percent").getDouble("MLT_SEARCHES");
        double prediction_conversion_rate = config.getJSONObject("totalConversionByUseCase_percent").getDouble("PREDICTION_SEARCHES");

        double hard_conversion_rate = config.getJSONObject("hardSoftConversion_split_percent").getDouble("hard");
        double soft_conversion_rate = config.getJSONObject("hardSoftConversion_split_percent").getDouble("soft");

        int totalEvents = config.getInt("totalEvents");

        int expected_total_prefix_conversion_count = (int) (totalEvents * prefix_conversion_rate);
        int expected_total_fulltext_conversion_count = (int) (totalEvents * fulltext_conversion_rate);
        int expected_total_mlt_conversion_count = (int) (totalEvents * mlt_conversion_rate);
        int expected_total_predictions_conversion_count = (int) (totalEvents * prediction_conversion_rate);

        /* gather all statistic for prefix search events and conversion */
        int expected_conversion_prefix_views_count = (int) (expected_total_prefix_conversion_count * views_conversion_rate);
        int expected_conversion_prefix_clicks_count = (int) (expected_total_prefix_conversion_count * clicks_conversion_rate);
        int expected_conversion_prefix_purchases_count = (int) (expected_total_prefix_conversion_count * purchase_conversion_rate);
        int expected_conversion_prefix_recordings_count = (int) (expected_total_prefix_conversion_count * record_conversion_rate);

        int expected_hard_conversion_by_prefix_views = (int) (expected_conversion_prefix_views_count * hard_conversion_rate);
        int expected_soft_conversion_by_prefix_views = (int) (expected_conversion_prefix_views_count * soft_conversion_rate);
        int expected_hard_conversion_by_prefix_clicks = (int) (expected_conversion_prefix_clicks_count * hard_conversion_rate);
        int expected_soft_conversion_by_prefix_clicks = (int) (expected_conversion_prefix_clicks_count * soft_conversion_rate);
        int expected_hard_conversion_by_prefix_purchases = (int) (expected_conversion_prefix_purchases_count * hard_conversion_rate);
        int expected_soft_conversion_by_prefix_purchases = (int) (expected_conversion_prefix_purchases_count * soft_conversion_rate);
        int expected_hard_conversion_by_prefix_recordings = (int) (expected_conversion_prefix_recordings_count * hard_conversion_rate);
        int expected_soft_conversion_by_prefix_recordings = (int) (expected_conversion_prefix_recordings_count * soft_conversion_rate);



        /* gather all statistic for fulltext search events and conversion */
        int expected_conversion_fulltext_views_count = (int) (expected_total_fulltext_conversion_count * views_conversion_rate);
        int expected_conversion_fulltext_clicks_count = (int) (expected_total_fulltext_conversion_count * clicks_conversion_rate);
        int expected_conversion_fulltext_purchases_count = (int) (expected_total_fulltext_conversion_count * purchase_conversion_rate);
        int expected_conversion_fulltext_recordings_count = (int) (expected_total_fulltext_conversion_count * record_conversion_rate);

        int expected_hard_conversion_by_fulltext_views = (int) (expected_conversion_fulltext_views_count * hard_conversion_rate);
        int expected_soft_conversion_by_fulltext_views = (int) (expected_conversion_fulltext_views_count * soft_conversion_rate);
        int expected_hard_conversion_by_fulltext_clicks = (int) (expected_conversion_fulltext_clicks_count * hard_conversion_rate);
        int expected_soft_conversion_by_fulltext_clicks = (int) (expected_conversion_fulltext_clicks_count * soft_conversion_rate);
        int expected_hard_conversion_by_fulltext_purchases = (int) (expected_conversion_fulltext_purchases_count * hard_conversion_rate);
        int expected_soft_conversion_by_fulltext_purchases = (int) (expected_conversion_fulltext_purchases_count * soft_conversion_rate);
        int expected_hard_conversion_by_fulltext_recordings = (int) (expected_conversion_fulltext_recordings_count * hard_conversion_rate);
        int expected_soft_conversion_by_fulltext_recordings = (int) (expected_conversion_fulltext_recordings_count * soft_conversion_rate);


         /* gather all statistic for mlt use case search events and conversion */
        int expected_conversion_mlt_views_count = (int) (expected_total_mlt_conversion_count * views_conversion_rate);
        int expected_conversion_mlt_clicks_count = (int) (expected_total_mlt_conversion_count * clicks_conversion_rate);
        int expected_conversion_mlt_purchases_count = (int) (expected_total_mlt_conversion_count * purchase_conversion_rate);
        int expected_conversion_mlt_recordings_count = (int) (expected_total_mlt_conversion_count * record_conversion_rate);

        int expected_hard_conversion_by_mlt_views = (int) (expected_conversion_mlt_views_count * hard_conversion_rate);
        int expected_soft_conversion_by_mlt_views = (int) (expected_conversion_mlt_views_count * soft_conversion_rate);
        int expected_hard_conversion_by_mlt_clicks = (int) (expected_conversion_mlt_clicks_count * hard_conversion_rate);
        int expected_soft_conversion_by_mlt_clicks = (int) (expected_conversion_mlt_clicks_count * soft_conversion_rate);
        int expected_hard_conversion_by_mlt_purchases = (int) (expected_conversion_mlt_purchases_count * hard_conversion_rate);
        int expected_soft_conversion_by_mlt_purchases = (int) (expected_conversion_mlt_purchases_count * soft_conversion_rate);
        int expected_hard_conversion_by_mlt_recordings = (int) (expected_conversion_mlt_recordings_count * hard_conversion_rate);
        int expected_soft_conversion_by_mlt_recordings = (int) (expected_conversion_mlt_recordings_count * soft_conversion_rate);


        /* gather all statistic for experiment use case search events and conversion */
        int expected_conversion_predictions_views_count = (int) (expected_total_predictions_conversion_count * views_conversion_rate);
        int expected_conversion_predictions_clicks_count = (int) (expected_total_predictions_conversion_count * clicks_conversion_rate);
        int expected_conversion_predictions_purchases_count = (int) (expected_total_predictions_conversion_count * purchase_conversion_rate);
        int expected_conversion_predictions_recordings_count = (int) (expected_total_predictions_conversion_count * record_conversion_rate);

        int expected_hard_conversion_by_predictions_views = (int) (expected_conversion_predictions_views_count * hard_conversion_rate);
        int expected_soft_conversion_by_predictions_views = (int) (expected_conversion_predictions_views_count * soft_conversion_rate);
        int expected_hard_conversion_by_predictions_clicks = (int) (expected_conversion_predictions_clicks_count * hard_conversion_rate);
        int expected_soft_conversion_by_predictions_clicks = (int) (expected_conversion_predictions_clicks_count * soft_conversion_rate);
        int expected_hard_conversion_by_predictions_purchases = (int) (expected_conversion_predictions_purchases_count * hard_conversion_rate);
        int expected_soft_conversion_by_predictions_purchases = (int) (expected_conversion_predictions_purchases_count * soft_conversion_rate);
        int expected_hard_conversion_by_predictions_recordings = (int) (expected_conversion_predictions_recordings_count * hard_conversion_rate);
        int expected_soft_conversion_by_predictions_recordings = (int) (expected_conversion_predictions_recordings_count * soft_conversion_rate);


        String csv_data = "";
        csv_data = csv_data.format("Conversion Rates By Type,,Start Time: "+helper.getLocalDateTime(startTime)+",End Time: "+helper.getLocalDateTime(endTime)+",,,\n");
        csv_data+= csv_data.format("Views,"+views_conversion_rate+",,,,,\n");
        csv_data+= csv_data.format("Click Through,"+record_conversion_rate+",,,,,\n");
        csv_data+= csv_data.format("Purchases,"+clicks_conversion_rate+",,,,,\n");
        csv_data+= csv_data.format("Recordings,"+record_conversion_rate+",,,,,\n");
        csv_data+= csv_data.format("Total Events,"+totalEvents+",,,,,\n");

        csv_data+= csv_data.format(",,,,,,\n");
        csv_data+= csv_data.format("Conversion Rates By Use Case,,,,,,\n");
        csv_data+= csv_data.format("Prefix Search,"+prefix_conversion_rate+",,,,,\n");
        csv_data+= csv_data.format("MLT (Query),"+mlt_conversion_rate+",,,,,\n");
        csv_data+= csv_data.format("Full Text,"+fulltext_conversion_rate+",,,,,\n");
        csv_data+= csv_data.format("Predictions,"+prediction_conversion_rate+",,,,,\n");

        csv_data+= csv_data.format(",,,,,,\n");
        csv_data+= csv_data.format("Hard Conversion Rate,"+hard_conversion_rate+",,,,,\n");
        csv_data+= csv_data.format("Soft Conversion Rate,"+soft_conversion_rate+",,,,,\n");

        csv_data+= csv_data.format(",,,,,,\n");
        csv_data+= csv_data.format("Expected Conversion By Use Case,,,,,,\n");
        csv_data+= csv_data.format("Prefix,"+expected_total_prefix_conversion_count+",,,,,\n");
        csv_data+= csv_data.format("MLT (Query),"+expected_total_mlt_conversion_count+",,,,,\n");
        csv_data+= csv_data.format("Full Text,"+expected_total_fulltext_conversion_count+",,,,,\n");
        csv_data+= csv_data.format("Predictions,"+expected_total_predictions_conversion_count+",,,,,\n");

        csv_data+= csv_data.format(",,,,,,\n");
        csv_data+= csv_data.format(",,,,,,\n");
        csv_data+= csv_data.format("Expected Conversion Rates By Use Case → Type (Prefix),,Expected Hard Conversion,Expected Soft Conversion,,Expected Hard Conversion Rate,Expected Soft Conversion Rate\n");

        //set prefix columns
        csv_data+=("Views,"+expected_conversion_prefix_views_count+","+expected_hard_conversion_by_prefix_views+","+expected_soft_conversion_by_prefix_views+",,"+((double) expected_hard_conversion_by_prefix_views/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_prefix_views/totalEvents)*100.0+"%\n");
        csv_data+=("Clicks,"+expected_conversion_prefix_clicks_count+","+expected_hard_conversion_by_prefix_clicks+","+expected_soft_conversion_by_prefix_clicks+",,"+((double) expected_hard_conversion_by_prefix_clicks/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_prefix_clicks/totalEvents)*100.0+"%\n");
        csv_data+=("Purchase,"+expected_conversion_prefix_purchases_count+","+expected_hard_conversion_by_prefix_purchases+","+expected_soft_conversion_by_prefix_purchases+",,"+((double) expected_hard_conversion_by_prefix_purchases/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_prefix_purchases/totalEvents)*100.0+"%\n");
        csv_data+=("Recordings,"+expected_conversion_prefix_recordings_count+","+expected_hard_conversion_by_prefix_recordings+","+expected_soft_conversion_by_prefix_recordings+",,"+((double) expected_hard_conversion_by_prefix_recordings/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_prefix_recordings/totalEvents)*100.0+"%\n");

        csv_data+=(",,,,,,\n");

        //set mlt columns
        csv_data+=("Expected Conversion Rates By Use Case → Type (MLT),,,,,Expected Hard Conversion Rate,Expected Soft Conversion Rate\n");
        csv_data+=("Views,"+expected_conversion_mlt_views_count+","+expected_hard_conversion_by_mlt_views+","+expected_soft_conversion_by_mlt_views+",,"+((double) expected_hard_conversion_by_mlt_views/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_mlt_views/totalEvents)*100.0+"%\n");
        csv_data+=("Clicks,"+expected_conversion_mlt_clicks_count+","+expected_hard_conversion_by_mlt_clicks+","+expected_soft_conversion_by_mlt_clicks+",,"+((double) expected_hard_conversion_by_mlt_clicks/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_mlt_clicks/totalEvents)*100.0+"%\n");
        csv_data+=("Purchase,"+expected_conversion_mlt_purchases_count+","+expected_hard_conversion_by_mlt_purchases+","+expected_soft_conversion_by_mlt_purchases+",,"+((double) expected_hard_conversion_by_mlt_purchases/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_mlt_purchases/totalEvents)*100.0+"%\n");
        csv_data+=("Recordings,"+expected_conversion_mlt_recordings_count+","+expected_hard_conversion_by_mlt_recordings+","+expected_soft_conversion_by_mlt_recordings+",,"+((double) expected_hard_conversion_by_mlt_recordings/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_mlt_recordings/totalEvents)*100.0+"%\n");
        csv_data+=(",,,,,,\n");

        //set fulltext columns
        csv_data+=("Expected Conversion Rates By Use Case → Type (FullText),,,,,Expected Hard Conversion Rate,Expected Soft Conversion Rate\n");
        csv_data+=("Views,"+expected_conversion_fulltext_views_count+","+expected_hard_conversion_by_fulltext_views+","+expected_soft_conversion_by_fulltext_views+",,"+((double) expected_hard_conversion_by_fulltext_views/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_fulltext_views/totalEvents)*100.0+"%\n");
        csv_data+=("Clicks,"+expected_conversion_fulltext_clicks_count+","+expected_hard_conversion_by_fulltext_clicks+","+expected_soft_conversion_by_fulltext_clicks+",,"+((double) expected_hard_conversion_by_fulltext_clicks/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_fulltext_clicks/totalEvents)*100.0+"%\n");
        csv_data+=("Purchase,"+expected_conversion_fulltext_purchases_count+","+expected_hard_conversion_by_fulltext_purchases+","+expected_soft_conversion_by_fulltext_purchases+",,"+((double) expected_hard_conversion_by_fulltext_purchases/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_fulltext_purchases/totalEvents)*100.0+"%\n");
        csv_data+=("Recordings,"+expected_conversion_fulltext_recordings_count+","+expected_hard_conversion_by_fulltext_recordings+","+expected_soft_conversion_by_fulltext_recordings+",,"+((double) expected_hard_conversion_by_fulltext_recordings/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_fulltext_recordings/totalEvents)*100.0+"%\n");
        csv_data+=(",,,,,,\n");


        //set predictions columns
        csv_data+=("Expected Conversion Rates By Use Case → Type (Predictions),,,,,Expected Hard Conversion Rate,Expected Soft Conversion Rate\n");
        csv_data+=("Views,"+expected_conversion_predictions_views_count+","+expected_hard_conversion_by_predictions_views+","+expected_soft_conversion_by_predictions_views+",,"+((double) expected_hard_conversion_by_predictions_views/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_predictions_views/totalEvents)*100.0+"%\n");
        csv_data+=("Clicks,"+expected_conversion_predictions_clicks_count+","+expected_hard_conversion_by_predictions_clicks+","+expected_soft_conversion_by_predictions_clicks+",,"+((double) expected_hard_conversion_by_predictions_clicks/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_predictions_clicks/totalEvents)*100.0+"%\n");
        csv_data+=("Purchase,"+expected_conversion_predictions_purchases_count+","+expected_hard_conversion_by_predictions_purchases+","+expected_soft_conversion_by_predictions_purchases+",,"+((double) expected_hard_conversion_by_predictions_purchases/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_predictions_purchases/totalEvents)*100.0+"%\n");
        csv_data+=("Recordings,"+expected_conversion_predictions_recordings_count+","+expected_hard_conversion_by_predictions_recordings+","+expected_soft_conversion_by_predictions_recordings+",,"+((double) expected_hard_conversion_by_predictions_recordings/totalEvents)*100.0+"%,"+((double) expected_soft_conversion_by_predictions_recordings/totalEvents)*100.0+"%\n");
        csv_data+=(",,,,,,\n");


        //set experiment columns
        if((experimentRan.length!=0 && experimentRan[0])){
            csv_data+="Summary for Campaign (Experiment):"+config.getString("experimentName")+",,,,,,\n";
            csv_data+="Total Impressions,"+getTotalExperimentImpressions()+",(total impressions by all users regardless of group),,,,\n";
            csv_data+="Unique Impressions,"+getTotalUniqueExperimentImpressions()+",(users saw it within top 10 limit if the experiment is activated regardless of group),,,,\n";
            csv_data+="Target Group Hard Conversions,"+getTotalTargetGroupHardConversions()+",(Users who were part of the target group and made hard conversion on the promotedId ),,,,\n";
            csv_data+="Target Group Soft Conversions,"+getTotalTargetGroupSoftConversions()+",(Users who were part of the target group and made hard conversion on the promotedId ),,,,\n";
            csv_data+="Unique Conversions,"+getUniqueConversionsByAllUsers()+",(unique users who are in experiment who saw it and converted regardless of group),,,,\n";
            csv_data+="Control Group Activated Count,"+activatedControlGroupCount+",(number of times the control group was activated),,,,\n";
            csv_data+="Target Group Activated Count,"+activatedTargetGroupCount+",(number of times the target group was activated),,,,\n";
            csv_data+="Background Group Activated Count,"+activatedBackgroundGroupCount+",(number of times the background group was activated),,,,\n";
            csv_data+="Experiment Promotion Id,"+config.getJSONArray("experimentTitleIds_ToConvert").toString()+",,,,,\n";
            csv_data+=(",,,,,,\n");
        }

        File csvFile = new File(System.getProperty("user.dir")+"/tmp/UseCasePerformance_Expected_Conversion_Stat-"+helper.getLocalDateTime(System.currentTimeMillis())+".csv");
        FileWriter write = new FileWriter(csvFile);
        write.write(csv_data);
        write.close();
        System.out.println("Total deviceIdCount is "+getTotalDeviceCounts_unqiue());
        System.out.println("Total profileIdCount is "+ getTotalProfileCounts_unqiue());
        System.out.println("Total device input is "+getDeviceInputs());
        System.out.println("See expected conversion breakdown at "+ csvFile.getAbsolutePath()+"\n");
    }


    //returns the activated variationName i.e (background group | control group | target group)
    //side effect - increments the activated group count -- this will add up to total impression count per variation group
    public String getActivatedExperimentVariationName(JSONObject debugResponse) throws Exception{
        JSONObject queryEvents = debugResponse.getJSONObject("debug").getJSONObject("queryEvent");
        String variationName = null;
        try {
            variationName = queryEvents.getJSONArray("experimentActivations").getJSONObject(0).getString("variationName"); // this assumes only one experiment (campaign mode)
            if(variationName.contains("(background group)")){
             activatedBackgroundGroupCount++;
            } else if(variationName.contains("target group")){
             activatedTargetGroupCount++;
            } else { activatedControlGroupCount++;
            }
            if(config.getBoolean("print_experimentLog")) System.out.printf("Activated Experiment Group: %s\n",variationName);
        } catch (Exception e){
            //there was no experiment activation for the current search
        }
        return variationName;
    }


    /* track impression counts for BG, TG, and CG group total impressions  - while we can get total impression per group with activatedGroupNameCount -- we do not know how many in those group are unique user impressions
    * this map of group to users will provide us that level of detail*/
    public static volatile Map <String, Set> campaign_impressions = new HashMap<>();

    /*
    set impression per variationGroup name - storing in the campaign_impressions map that give us detail about how many unique impressions per variation {control, target, background} by conversionType {clicks, purchase, record, view}
     */
    public void setVariationGroupImpressions(String variationGroupName,ConversionType conversionType, String userId) throws Exception {
        String variationGroupConversionTypeKey = variationGroupName+"_"+conversionType;
        if(campaign_impressions.get(variationGroupConversionTypeKey)==null){
            Set users = new HashSet();
            users.add(userId);
            campaign_impressions.put(variationGroupConversionTypeKey,users);
        } else {
            Set users = campaign_impressions.get(variationGroupConversionTypeKey);
            users.add(userId);
            campaign_impressions.put(variationGroupConversionTypeKey,users);
        }
    }


    private static volatile int activatedControlGroupCount = 0;
    private static volatile int activatedTargetGroupCount = 0;
    private static volatile int activatedBackgroundGroupCount = 0;
    volatile  Map <String, Integer> experiment_conversionCounts = new HashMap<>(); //store conversion counts just for experiment based events
    volatile  Map <String, Integer> useCase_conversionCounts = new HashMap<>();


   /* get unique impressions for an experiment if one was ran
   * for each of the impression group -- add up their hash set size to get unique user impressions across all the variations*/
   public int getTotalUniqueExperimentImpressions(){
       int overallImpressions = 0;
       for(String variationImpression: campaign_impressions.keySet()){
           Set uniqueUsersInVariation = campaign_impressions.get(variationImpression);
           overallImpressions += uniqueUsersInVariation.size();
       }
        return  overallImpressions;
   }

   /*returns the total impressions across all variation group -- duplicates are counted
   this is actually the number of total activated variations
    */
   public int getTotalExperimentImpressions(){
       return activatedBackgroundGroupCount+activatedControlGroupCount+activatedTargetGroupCount;
   }

    /* the sum of all conversions from unique users who got an impression (experiment activated) and converted
    regardless of variation group, if this is not an experiment, get the conversion counts from the useCase_conversionCounts and add to experiment_conversionCounts
    since it is possible that experiments got activated by nature of our random searching and user*/
    public int getUniqueConversionsByAllUsers(){
       //each variationConversion group has a count of how many conversions were made.. but not necessarily by what users
        // the campaign impression might give the answer instead of this.. but we didnt' convert on every impression
       int totalConversions = 0;
        for(String variationConversion: experiment_conversionCounts.keySet()){
          totalConversions+= experiment_conversionCounts.get(variationConversion);
       }
       //this is not an experiment, add the conversions from the useCase map
       if(!config.getBoolean("enableExperiments")){
            for(String conversionType: useCase_conversionCounts.keySet()){
                totalConversions+=useCase_conversionCounts.get(conversionType);
            }
       }

       return totalConversions;
    }

    public String getRandomSearchTerm() throws Exception {
        if(searchTermOverrides.length==0){
            return helper.getRandomTitleFromTivoCore().get("title").toString();
        }
        else {
            //mix or interleave in some random core terms with our fixed searchTermOverrides
            //but only do so if we configured the property for it
            if(mixSearchTermWithRandomCoreTitles){
                if(new Random().nextBoolean()){
                    return helper.getRandomTitleFromTivoCore().get("title").toString();
                }
            }
            return searchTermOverrides[new Random().nextInt(searchTermOverrides.length)].replace(" ","%20");
        }
    }

    public String getRandomPrefixSearchTerm() throws Exception{
        if(searchTermOverrides.length==0){
            return helper.getRandomPrefixSearchTerms();
        }
        else {
            //mix or interleave in some random core terms with our fixed searchTermOverrides
            //but only do so if we configured the property for it
            if(mixSearchTermWithRandomCoreTitles){
                if(new Random().nextBoolean()){
                    return helper.getRandomTitleFromTivoCore().get("title").toString();
                }
            }
            return searchTermOverrides[new Random().nextInt(searchTermOverrides.length)].replace(" ","%20");
        }
    }

    public String getRandomSearchId() throws Exception{
        if(searchIds.length==0){
            return helper.getRandomTitleFromTivoCore().get("id").toString();
        }
        else {
            if(searchIds.length>1) return searchIds[new Random().nextInt(searchIds.length)];
            return searchIds[0];
        }
    }

    public JSONObject setStationId(JSONObject data, String itemId) throws Exception{
        if(postStationId){
            String stationId = helper.getStationId(itemId);
            if(helper.getStationId(itemId)!=null) data.put("stationId",stationId);
        }
        return data;
    }


    //sum up the hard conversion for all conversion type for target group
    public int getTotalTargetGroupHardConversions() throws Exception{
        int totalTargethardConversion=0;
        for(String targetConversion: experiment_conversionCounts.keySet()){
            if(targetConversion.contains("target") && targetConversion.contains("HARD_CONVERSION")){
                totalTargethardConversion+=experiment_conversionCounts.get(targetConversion);
            }
        }
        return totalTargethardConversion;
    }

    //sum up the soft conversion for all conversion type for target group
    public int getTotalTargetGroupSoftConversions() throws Exception{
        int totalTargethardConversion=0;
        for(String targetConversion: experiment_conversionCounts.keySet()){
            if(targetConversion.contains("target") && targetConversion.contains("SOFT_CONVERSION")){
                totalTargethardConversion+=experiment_conversionCounts.get(targetConversion);
            }
        }
        return totalTargethardConversion;
    }

    /*
    total number of number of devices by each user --- this number is not used by etl, but good to see what it is to for debugging
     */
    public int getDeviceInputs() throws Exception{
        int totalDeviceprovided = 0;
        for (String user: userDeviceIds.keySet()){
            totalDeviceprovided+=userDeviceIds.get(user).size();
        }
        return totalDeviceprovided;
    }

    /*get total deviceId -- (1) regardless of users, the deviceId counts are de-duplicated according to Carl, meaning if user1 sent in view with deviceId=dev1
     and user2 sent in view same deviceId -- the audience measurement would count that as total of 1 device rather than 2 separate device.
     The same holds for profileId.
     (2) If the same user sent in various views with the same deviceId, the total device count does not go up, stays the same because it audience meas. looks at unique deviceId
    */
    public int getTotalDeviceCounts_unqiue() throws Exception{
        return uniqueDeviceIdSet.size();
    }

    /*get total profileId -- (1) regardless of users, the deviceId counts are de-duplicated according to Carl, meaning if user1 sent in view with deviceId=dev1
     and user2 sent in view same deviceId -- the audience measurement would count that as total of 1 device rather than 2 separate device.
     The same holds for profileId.
     (2) If the same user sent in various views with the same deviceId, the total device count does not go up, stays the same because it audience meas. looks at unique deviceId
    */
    public int getTotalProfileCounts_unqiue() throws Exception{
        return uniqueProfileIdSet.size();
    }


    /*
    Replay query events from an avro file
    * */
    public static void re_executeQueryLogSearch(String queryLogAvroFile, QAHelper helper, String url) throws Exception{
        DataFileReader dataFileReader = helper.readAvroFile(null,queryLogAvroFile);
        GenericRecord user = null;
        Map <String, String> eventInfo = getAvroEventInfo(queryLogAvroFile);
        while (dataFileReader.hasNext()) {
            user = (GenericRecord) dataFileReader.next(user);
            JSONArray path = new JSONArray(user.get("tapPaths").toString());
            String tap = path.getString(path.length()-1); //get the last tap path in the array -- that is final path to use for execution
            String params = user.get("params").toString();
            String searchURL = url+"/sd/"+eventInfo.get("pop")+"/taps/"+tap+"?"+params;
            System.out.println(helper.sendGetEventToEndPoint(searchURL));
        }
    }

    /*
    replay some view events from avro file
    @Param replayAsToday defaults to false -- replays the event as is, if set to true, change the event interval being replay to any time within the last 12 hours
     */
    public static void replay_viewEvents(String viewEventsAvroFile, QAHelper helper, String url,boolean replayAsToday) throws Exception{
        DataFileReader dataFileReader = helper.readAvroFile(null,viewEventsAvroFile);
        GenericRecord user = null;
        Map <String, String> eventInfo = getAvroEventInfo(viewEventsAvroFile);
        while (dataFileReader.hasNext()) {
            user = (GenericRecord) dataFileReader.next(user);
            JSONObject view = new JSONObject(user.toString());
            //we're playing with invalid views
            if(view.toString().contains("constraintViolations")){
                view = new JSONObject(user.get("view").toString());
            }

            long newStartTime = view.getLong("startTime");
            long newEndTime = view.getLong("endTime");

            if(replayAsToday){
                newStartTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(new Random().nextInt(12)); //some random time within the last 12 hour
                newEndTime = newStartTime + TimeUnit.MINUTES.toMillis(new Random().longs(5,60).findFirst().getAsLong()); // new duration somewhere between 5 minutes to an hour
            }

            view.put("startTime",newStartTime);
            view.put("endTime",newEndTime);
            String viewEventsURL =url+"/sd/"+eventInfo.get("pop")+"/"+eventInfo.get("eventType");
            helper.postEventToEndpoint("["+view.toString()+"]",viewEventsURL,true);
        }

    }


    /*
    generic replay events -- recursively plays all event from a yyyy-mm-dd
     */
    public static void replay_eventsFromAllDir(String dirPath, QAHelper helper, String url,AvroEventType avroEventType,boolean replayAsToday) throws Exception{
        File folder = new File(dirPath);
        File[] listOfFiles = null;
        if(!folder.isDirectory()){
            switch (avroEventType){
                case VIEWS:
                    replay_viewEvents(folder.getAbsolutePath(),helper,url,replayAsToday);
                    break;
                case QUERYLOGS:
                    re_executeQueryLogSearch(folder.getAbsolutePath(),helper,url);
                    break;
                case USER_ATTRIBUTES:
                    updateRandomAttributeUpdates(folder.getAbsolutePath(),helper,url);
                    break;
                default:
                    break;
            }
        } else {
            listOfFiles = folder.listFiles();
            for(File file: listOfFiles){
                replay_eventsFromAllDir(file.getAbsolutePath(),helper,url,avroEventType,replayAsToday);
            }
        }
    }

    /*
    update use profile with some random attributes given a view-event avro -- pull out some useful events and change
    @Param viewEventsAvroDir should be the time bucket folder for the view-events log
    @todo: make this use the real user-profile avros if one exist. Most staging or even prod environment do not have that data,
     so ok to extract from views avro generate random use update at the moment
    */
    public static void updateRandomAttributeUpdates(String viewEventsAvroDir, QAHelper helper, String url) throws Exception{

        GenericRecord user = null;
        File avroFolder = new File(viewEventsAvroDir);
        File [] listOfFiles = avroFolder.listFiles(); //get all the avros from the current time bucket
        for(File file: listOfFiles){
            DataFileReader dataFileReader = helper.readAvroFile(null,file.getAbsolutePath());
            Map <String, String> eventInfo = getAvroEventInfo(file.getAbsolutePath());
            while (dataFileReader.hasNext()) {
                user = (GenericRecord) dataFileReader.next(user);
                JSONObject user_attribute = new JSONObject();
                user_attribute.put("id",user.get("userId").toString());
                user_attribute.put("attributes",new JSONObject());
                user_attribute.getJSONObject("attributes").put("zipCode",user.get("postalCode"));
                user_attribute.getJSONObject("attributes").put("country",user.get("country"));
                user_attribute.getJSONObject("attributes").put("currentSubscriber",new Random().nextBoolean());
                String viewEventsURL =url+"/sd/"+eventInfo.get("pop")+"/users/"+user.get("userId").toString();
                helper.putEventToEndpoint(user_attribute.toString(),viewEventsURL ,true);
            }
        }
    }


    /*
    return an information map about the avro file i.e the event type, the population name
     */
    public static Map<String, String> getAvroEventInfo(String avroFileName) throws Exception {
        //assumption that a file is always greating than 13 may be incorrect -- this assumes some sort of timestamp appending to every avro file naming
        if (!(avroFileName!=null && avroFileName.length()>13 && avroFileName.contains("-"))) {
            System.out.println("avro file does not mat expected format popName-eventType-timestamp.avro");
            return null;
        }
        Map <String, String> info = new HashMap<>();
        String populationName = "default";
        String eventType = "querylog"; //default to querylog

        populationName =  avroFileName.split("/")[avroFileName.split("/").length-1]; // get only the file name -- remove dirs
        populationName = populationName.substring(0,populationName.lastIndexOf("-")); // get only the popName + eventType

        if(populationName.contains("view-events")){
            populationName = populationName.substring(0,populationName.lastIndexOf("view-events")-1);
            eventType = "viewEvents";
        } else if(populationName.contains("invalid-views")){
            populationName = populationName.substring(0,populationName.lastIndexOf("invalid-views")-1);
            eventType = "viewEvents";
        }
        else if(populationName.contains("querylogs")){
            populationName = populationName.substring(0,populationName.lastIndexOf("querylogs")-1);
        }

        info.put("pop",populationName);
        info.put("eventType",eventType);

        return info;
    }

    public enum LookAheadType {MIN,MAX}

    public int getLookAheadTime(UseCase useCase, ConversionType conversionType, JSONObject config,LookAheadType lookType) throws Exception{
        int lookIndex = 0;
        if(lookType.equals(LookAheadType.MAX)){
            lookIndex = 1;
        }
        int defaultLookAhead = 31; //defaults 30 minute (31 because nextInt() from Random excludes upper value)
        JSONObject lookAheadConfig = new JSONObject();
        try {
            lookAheadConfig = config.getJSONObject("lookAheadTime_minutes");
            //set global defaults
            try{
                switch (conversionType){
                    case VIEWS:
                        defaultLookAhead = lookAheadConfig.getJSONArray("VIEW").getInt(lookIndex);
                        break;
                    case RECORD:
                        defaultLookAhead = lookAheadConfig.getJSONArray("RECORD").getInt(lookIndex);
                        break;
                    case PURCHASE:
                        defaultLookAhead = lookAheadConfig.getJSONArray("PURCHASE").getInt(lookIndex);
                        break;
                    case CLICK_THROUGH:
                        defaultLookAhead = lookAheadConfig.getJSONArray("CLICK_THROUGH").getInt(lookIndex);
                        break;
                }
            } catch (Exception e){
                //no defaults found in lookAheadTime config
            }

            //set UseCase specific lookAhead SI-2860
            switch(useCase) {
                case PREDICTIONS:
                    switch (conversionType){
                        case VIEWS:
                            defaultLookAhead = lookAheadConfig.getJSONObject("PREDICTIONS").getJSONArray("VIEW").getInt(lookIndex);
                            return defaultLookAhead;
                        case RECORD:
                            defaultLookAhead = lookAheadConfig.getJSONObject("PREDICTIONS").getJSONArray("RECORD").getInt(lookIndex);
                            return defaultLookAhead;
                        case PURCHASE:
                            defaultLookAhead = lookAheadConfig.getJSONObject("PREDICTIONS").getJSONArray("PURCHASE").getInt(lookIndex);
                            return defaultLookAhead;
                        case CLICK_THROUGH:
                            defaultLookAhead = lookAheadConfig.getJSONObject("PREDICTIONS").getJSONArray("CLICK_THROUGH").getInt(lookIndex);
                            return defaultLookAhead;
                    }
                case PREFIX_SEARCH: //based on SI-2860 prefix & full text gets the same, which is the default.
                    // For anything else, just use the default global settings
                    switch (conversionType){
                        case VIEWS:
                            defaultLookAhead = lookAheadConfig.getJSONObject("PREFIX_SEARCH").getJSONArray("VIEW").getInt(lookIndex);
                            return defaultLookAhead;
                        case RECORD:
                            defaultLookAhead = lookAheadConfig.getJSONObject("PREFIX_SEARCH").getJSONArray("RECORD").getInt(lookIndex);
                            return defaultLookAhead;
                        case PURCHASE:
                            defaultLookAhead = lookAheadConfig.getJSONObject("PREFIX_SEARCH").getJSONArray("PURCHASE").getInt(lookIndex);
                            return defaultLookAhead;
                        case CLICK_THROUGH:
                            defaultLookAhead = lookAheadConfig.getJSONObject("PREFIX_SEARCH").getJSONArray("CLICK_THROUGH").getInt(lookIndex);
                            return defaultLookAhead;
                    }
            }
        } catch (Exception e){
            return defaultLookAhead;
        }

        return defaultLookAhead;
    }

}