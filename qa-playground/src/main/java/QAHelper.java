import org.apache.avro.io.DatumReader;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.inference.TestUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import javax.json.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by dlam
 * This is a tool to help with Catalog testing.
 * It can generate large test data at least 18 different Catalog objects that can be used for update or replacement
 * Use the tool to send consecutive updates or bulk replacement of any size -- in conjunction with HttpHelper, can use to monitor queries against SD under traffic load of catalog update/replacements
 * https://unsplash.it/ --images if we really care to get fake image data
 */
public class QAHelper {
    final static int CONTENT_DURATION[] = {5, 10, 30, 60, 120, 180, 360,};
    final static String local_catServerUpdate = "http://localhost:8888/catalogs/lock/updates";
    final static String local_catServerReplace = "http://localhost:8888/catalogs/sales-demo/replace/";
    final static String catServerUpdate = "http://internal-app-network.net:8888/catalogs/lock/updates";
    final static String catServerReplace = "http://internal-app-network.net:8888/catalogs";
    final static String ffsBaseServer = "http://internal-app-network.net";
    final static String[] titleSeederES = {"la fresá", "la sandía", "el anna", "el damascó", "el dúranzo", "el arándano", "el pelón", "la frambuésa", "¡Dígaselo", "Tíerra", "el limón", "¿Quién?", "ñino", "estüvo"};
    final static String[] titleSeederDE = {"der Mann", "die Frau", "das Mädchen", "Ich weiß nicht", "Brötchen", "Frei blÜe", "ut Bär"};
    final static String[] titleSeederFR = {"Je m'appelle", "Je veux être avec toi", "Ça va?", "S'il vous plaît", "Allons-y", "Merci", "Au revoir"};
    final static String[] titleSeederPT_BR = {"Até mais", "Até logo", "Não há de quê", "conhecê-lo", "você é", "Você fala inglês"};
    final static String[] titleSeederPT = {"Até mais", "Até logo", "Não há de quê", "conhecê-lo", "você é", "Você fala inglês"};
    final static String[] titleSeederPL = {"Jak się masz", "Dzień dobry", "Dobry wieczór", "Cześć", "Proszę", "Zatańczymy", "Jestem żonaty"};
    final static String[] titleSeederKH = {"ថ្ងៃចន្ទ", "កាលពីថ្ងៃអង្គារ", "ថ្ងៃពុធ", "ថ្ងៃព្រហស្បតិ៍", "កាលពីថ្ងៃសុក្រ", "ថ្ងៃសៅរ៍"};
    final static String[] titleSeederIT = {"città", "è Perù", "falò", "Amorì", "àncora", "nòcciolo", "sùbito", "affinché", "poiché", "perché"};
    final static String[] titleSeederEN = {"The Strawberry", "Watemelon", "Pineapple", "A Peach", "Nectarine", "A blueberry", "A grapefruit", "Avocado Beyond", "Above", "Earth", "Blue Moon"};
    final static String[] titleSeederZH = {"苹果", "橙子", "法式吐司", "蓝莓", "摩天大楼", "美式足球"};
    final static String[] titleSeederSV = {"Välkommen", "Hallå God dag", "Hur står det till", "Hur mår du", "Det var länge sedan vi sågs sist!", "Jag är från", "God kväll", "Ursäkta"};
    final static String[] genres = {"Action", "Absurdist", "Adventure", "Comedy", "Crime", "Drama", "Fantasy", "Historical", "Horror", "Mystery", "Romance", "Thriller", "Western", "Urban", "Satire", "Saga", "Science Fiction"};
    private String suffix = "2017";
    private static String TEAM_NAMES[] = {"Miami Marlins", "Marlens", "Marlene", "Little Jerry Seinfelds", "Grizzly Bear", "Hornets and Lady Hornets", "Falcons", "Falcrum"};
    private static String CREDIT_NAMES[] = {"Seth MacFarlane", "Leonardo DiCaprio", "Tom Hanks", "Brad Pits", "Johnny Depp", "Al Pacino", "Al Pacquo", "Tom Harold"};
    private static JSONArray TIVO_ASSETS_W_OFFERS = new JSONArray(); // load from data scrapped from catalog-tivo-assets-core
    private static JSONObject TIVO_CHANNEL_NETWORK = new JSONObject(); // load from station->channel/network mapping data scrapped from catalog-tivo-assets-core
    private String baseSearchURL = "";
    private int eventFromPastDays = 0;



    public static enum LANG {EN, ES, FR, DE, IT, PT_BR, KH, PT, PL, ZH, SV}

    private static String CHOSEN_LANG;
    private Long expireTime = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1); //for our test data - just expire everything we generate within 24 hours
    private static boolean createPreview = false;

    public static Map<Integer, String[]> langMap = new HashMap();
    private static String default_data = null;


    public String[] langTitleSeeds = {};
    public static boolean debug = false;

    String[] series, movies, episodes, topLevelImages;

    public static enum CatalogObjects {SportsEvent, SportsOrganization, CreativeWork_Series, CreativeWork_Program, Episode, LinearBlock, Offer, Station, Channel, VodCategory, VodOffer, VodPackage}

    public static enum OrgType {SPORT, LEAGUE, DIVISION, CONFERENCE, UNIVERSITY}

    private boolean hasSearchTag = false;
    private String searchTags = "QA Generated Search Tag";


    public QAHelper(LANG lang, Boolean debugOn, Boolean hasSearchTag, String suffix, Long expireTime) {
        this.hasSearchTag = hasSearchTag;
        setLangSeeder(lang.name());
        this.suffix = suffix;
        this.debug = debugOn;
        this.CHOSEN_LANG = lang.name();
        this.expireTime = expireTime;
        langMap.put(1, titleSeederEN);
        langMap.put(2, titleSeederES);
        langMap.put(3, titleSeederFR);
        langMap.put(4, titleSeederIT);
        langMap.put(5, titleSeederPT_BR);
        langMap.put(6, titleSeederKH);
        langMap.put(7, titleSeederPT);
        langMap.put(8, titleSeederPL);
        langMap.put(9, titleSeederZH);
        langMap.put(10, titleSeederSV);
        try {
            System.out.println("         ...Loading default test data.....");
            String filePath = System.getProperty("user.dir") + "/QAGeneratedCatalogData.json";
            String filePath_tivoAssets = System.getProperty("user.dir") + "/tmp/"+"catalog-tivo-demo-assets-core.json";
            String filePath_tivoStationMapping = System.getProperty("user.dir") + "/tmp/"+"channelNetworkMapping-catalog-tivo-demo-assets-core.json";
            this.default_data = convertFileContentToData(filePath);
            if(new File(filePath_tivoAssets).exists()){
                this.TIVO_ASSETS_W_OFFERS = new JSONArray(convertFileContentToData(filePath_tivoAssets));
                this.TIVO_CHANNEL_NETWORK = new JSONObject(convertFileContentToData(filePath_tivoStationMapping));
            }
            File tmpDataDir = new File(System.getProperty("user.dir") + "/tmp");
            tmpDataDir.mkdir();
        } catch (Exception e) {
        }
    }

    public void setLangSeeder(String lang) {
        switch (lang) {
            case "EN":
                langTitleSeeds = titleSeederEN;
                break;
            case "ES":
                langTitleSeeds = titleSeederES;
                break;
            case "FR":
                langTitleSeeds = titleSeederFR;
                break;
            case "DE":
                langTitleSeeds = titleSeederDE;
                break;
            case "IT":
                langTitleSeeds = titleSeederIT;
                break;
            case "PT_BR":
                langTitleSeeds = titleSeederPT_BR;
                break;
            case "KH":
                langTitleSeeds = titleSeederKH;
                break;
            case "PT":
                langTitleSeeds = titleSeederPT;
                break;
            case "PL":
                langTitleSeeds = titleSeederPL;
                break;
            case "ZH":
                langTitleSeeds = titleSeederZH;
                break;
            case "SV":
                langTitleSeeds = titleSeederSV;
                break;
            default:
                langTitleSeeds = titleSeederEN;
        }
    }

    public QAHelper(LANG lang, Boolean debugOn, Boolean hasSearchTag, String suffix) {
        this.hasSearchTag = hasSearchTag;
        switch (lang) {
            case EN:
                langTitleSeeds = titleSeederEN;
                break;
            case ES:
                langTitleSeeds = titleSeederES;
                break;
            case FR:
                langTitleSeeds = titleSeederFR;
                break;
            case DE:
                langTitleSeeds = titleSeederDE;
                break;
            case IT:
                langTitleSeeds = titleSeederIT;
                break;
            case PT_BR:
                langTitleSeeds = titleSeederPT_BR;
                break;
            case KH:
                langTitleSeeds = titleSeederKH;
                break;
            case PT:
                langTitleSeeds = titleSeederPT;
                break;
            case PL:
                langTitleSeeds = titleSeederPL;
                break;
            case ZH:
                langTitleSeeds = titleSeederZH;
                break;
            case SV:
                langTitleSeeds = titleSeederSV;
                break;
            default:
                langTitleSeeds = titleSeederEN;
        }
        this.suffix = suffix;
        this.debug = debugOn;
        this.CHOSEN_LANG = lang.name();
        try {
            this.default_data = convertFileContentToData("/home/data/_Work/Testings/QATools/QAGeneratedCatalogData.json");
        } catch (Exception e) {
        }
        this.initializeAuthenication();
    }

    public QAHelper() {
        this(LANG.EN, false, false, "TIVO", System.currentTimeMillis() + TimeUnit.DAYS.toMillis(365));
        this.initializeAuthenication();
    }


    //handle more bad values
    public static boolean parseBooleanInput(String value) {
        return value.matches("\\b[Yy](es)?|[Tt](rue)?\\b");
    }

    //generate Channel object to be embedded in channelLineup
    public JsonObject generateChannel(String channelId, String channelNum) {
        JsonObject channel = Json.createObjectBuilder().add("id", channelId)
                .add("altIds", Json.createArrayBuilder().add(generateAltId("tms.stationId", Optional.of(new Random().nextInt(50) + "1120"))).add(generateAltId("tms.stationId2", Optional.of(new Random().nextInt(50) + "1120")))
                ).
                        add("stationId", "tms:" + channelId).
                        add("channel", channelNum).
                        add("channelSort", channelNum.substring(1, 3)).
                        add("minorNum", channelNum.substring(1, 3)).
                        build();
        return channel;
    }

    public JsonObject generateVodPackage(String id, String title, int dateRange) {
        Map startEnd = getStartEndTime(dateRange);
        JsonObject vodPackage = Json.createObjectBuilder().add("type", "VodPackage").add("id", id).
                add("content", Json.createObjectBuilder().
                        add("id", id).
                        add("altIds", Json.createArrayBuilder().add(generateAltId("tms:series", Optional.of(title + "SH01009000")))).
                        add("type", "RENTAL").
                        add("startTime", startEnd.get("startTime").toString()).
                        add("endTime", startEnd.get("endTime").toString()).
                        add("title", Json.createObjectBuilder().add(CHOSEN_LANG, title)).
                        add("localities", Json.createArrayBuilder().add("test:local"))
                ).build();

        if (debug) jsonToString(vodPackage);
        return vodPackage;
    }


    public JsonObject generateVodCategory(String catId, String catName) {
        JsonObject vodCategory = Json.createObjectBuilder().add("type", "VodCategory").add("id", catId).
                add("content", Json.createObjectBuilder().
                        add("id", catId).
                        add("altIds", Json.createArrayBuilder().add(generateAltId("tms:series", Optional.of("SH01009396")))).
                        add("isRoot", JsonValue.TRUE).
                        add("name", Json.createObjectBuilder().add(CHOSEN_LANG, catName)).
                        add("children", Json.createArrayBuilder().add(Json.createObjectBuilder().add("childId", "637844776"))).
                        add("images", Json.createArrayBuilder().add(addImage("http://192.168.1.1:8001/poster/cat-637844633.jpg", true)))
                ).build();
        if (debug) jsonToString(vodCategory);
        return vodCategory;
    }


    public JsonObject generateCategory(String id, String name) {
        JsonObject category = Json.createObjectBuilder().add("type", "Category").add("id", id).
                add("content", Json.createObjectBuilder().
                        add("id", id).
                        add("altIds", Json.createArrayBuilder().add(generateAltId("tms:series", Optional.of("SH01009396")))).
                        add("isRoot", JsonValue.TRUE).
                        add("name", Json.createObjectBuilder().add(CHOSEN_LANG, name)).
                        add("children", Json.createArrayBuilder().add(Json.createObjectBuilder().add("childId", "637844776"))).
                        add("images", Json.createArrayBuilder().add(addImage("http://192.168.1.1:8001/poster/cat-637844633.jpg", true)))

                ).build();

        if (debug) jsonToString(category);
        return category;
    }


    //generate ChannelLineup and embedd the Channel object
    public JsonObject generateChannelLineup(String lineupId, String lineupName) {
        JsonObject channelLineup = Json.createObjectBuilder().
                add("expireTime", this.expireTime).
                add("type", "ChannelLineup").
                add("id", lineupId).
                add("content", Json.createObjectBuilder().
                        add("id", lineupId).
                        add("altIds", Json.createArrayBuilder().add(generateAltId("tms.lineupId", Optional.of("Alt-USA-Default"))).add(generateAltId("tms.lineupId2", Optional.of("Alt-USA-Default2")))).
                        add("lineupType", "CABLE").
                        add("name", Json.createObjectBuilder().add(CHOSEN_LANG, lineupName)).
                        add("channels", Json.createArrayBuilder().add(generateChannel("USA-KY16736-DEFAULT-22216-003", "003")).
                                add(generateChannel("USA-KY16736-DEFAULT-22216-004", "004")))
                )

                .build();
        if (debug) jsonToString(channelLineup);
        return channelLineup;
    }


    //generate a SportsEvent w/ altId -- include all child objects 1) SportsOrganization 2) SportsTeam 3) genres
    public JsonObject generateSportsEvent(String sportsId, String title, int id) {
        JsonObject SportsEvent = Json.createObjectBuilder().add("type", "SportsEvent").add("id", sportsId + id).
                add("content", Json.createObjectBuilder().add("altIds", Json.createArrayBuilder().add(Json.createObjectBuilder().add("ns", "ds:guid").add("id", UUID.randomUUID().toString()))).
                        add("id", sportsId + id).
                        add("releaseYear", 2017).
                        add("title", Json.createObjectBuilder().add(CHOSEN_LANG, "Sports Title - " + title)).
                        add("eventTitle", Json.createObjectBuilder().add(CHOSEN_LANG, "Event Title - " + title)).
                        add("genres", Json.createArrayBuilder().add(generateGenres(11, "Football", "alt:AmericanFootball", false))).
                        add("organizations", Json.createArrayBuilder().add(generateSportsOrganization("FootBall", "tivo:sp:911", false))).
                        add("teams", Json.createArrayBuilder().add(generateSportsTeam("tivo:tm.3001", TEAM_NAMES[new Random().nextInt(TEAM_NAMES.length - 1)], "Seattle", true, false))).
                        add("gameTime", "2017-02-28T15:30:00+00:00").
                        build()).
                build();
        if (debug) jsonToString(SportsEvent);
        return SportsEvent;
    }

    //generate Genres to be attached to other events w/ altIds included
    public JsonObject generateGenres(int genreId, String genreName, String altId, boolean returnAsParent) {
        JsonObject genres = Json.createObjectBuilder()
                .add("id", genreId + "")
                .add("name", Json.createObjectBuilder().add(CHOSEN_LANG, genreName))
                .add("altIds", Json.createArrayBuilder().add(Json.createObjectBuilder().add("ns", "tms:altGenreId").add("id", "Alt" + altId)))
                .build();

        if (returnAsParent) {
            genres = Json.createObjectBuilder()
                    .add("type", "Genre")
                    .add("id", genreId + "")
                    .add("content", Json.createObjectBuilder().add("id", genreId + "").add("name", Json.createObjectBuilder().add(CHOSEN_LANG, genreName))
                            .add("altIds", Json.createArrayBuilder().add(Json.createObjectBuilder().add("ns", "tms:altGenreId").add("id", "Alt" + altId)))
                    ).build();
            if (debug) jsonToString(genres);
        }

        return genres;
    }

    //generate searchTag object to be attached to SportsTeam & CreativeWorks object
    public JsonObject generateSearchTags(String tags) {
        JsonObject searchTags = Json.createObjectBuilder()
                .add("searchTags", tags)
                .build();
        return searchTags;
    }

    //generate SportsOrganization w/ altId that can be embedded into SportsEvents
    public JsonObject generateSportsOrganization(String orgName, String id, boolean returnAsParentObject) {
        JsonObject organizations = Json.createObjectBuilder().add("orgType", "SPORT").
                add("id", id + "").add("name", Json.createObjectBuilder().
                add(CHOSEN_LANG, orgName).build())
                .add("altIds", Json.createArrayBuilder().add(generateAltId("Alt" + orgName, Optional.of("SportsAltId:" + orgName)))
                ).build();

        if (returnAsParentObject) {
            organizations = Json.createObjectBuilder().add("type", "SportsOrganization").
                    add("id", id).
                    add("content", Json.createObjectBuilder().
                            add("id", id).
                            add("orgType", "SPORT").
                            add("name", Json.createObjectBuilder().add(CHOSEN_LANG, orgName))
                            .add("altIds", Json.createArrayBuilder().
                                    add(generateAltId("Alt" + orgName, Optional.of("SportsAltId:" + orgName))).build())).build();

            if (debug) jsonToString(organizations);
        }

        return organizations;
    }

    //generate SportsTeam w/ altId that can be embedded into SportsEvents
    public JsonObject generateSportsTeam(String id, String teamName, String location, boolean embedOrg, boolean returnAsParent) {
        if (returnAsParent) {
            System.out.println("Return as top-level object instead of embbedded object");
        }

        JsonObject team;
        if (hasSearchTag) {
            team = Json.createObjectBuilder().add("id", id).
                    add("name", Json.createObjectBuilder().add(CHOSEN_LANG, teamName)).
                    add("nickname", Json.createObjectBuilder().add(CHOSEN_LANG, "nickName - " + teamName)).
                    add("abbreviation", Json.createObjectBuilder().add(CHOSEN_LANG, teamName.substring(0, 3))).
                    add("location", Json.createObjectBuilder().add(CHOSEN_LANG, location)).
                    add("isHome", JsonValue.TRUE).
                    add("organizations", Json.createArrayBuilder().add(generateSportsOrganization("American Football", "tivo:sp.41", false))).
                    add("altIds", Json.createArrayBuilder().add(generateAltId("ds:altTeamId", Optional.of("Alt." + id))).add(generateAltId("ds:altTeamId2", Optional.of("Alt2." + id))))
                    .build();
        } else { // this is pretty dumb -- there is probably a better way to just insert one extra value instead of creating a whole new object
            team = Json.createObjectBuilder().add("id", id).
                    add("name", Json.createObjectBuilder().add(CHOSEN_LANG, teamName)).
                    add("nickname", Json.createObjectBuilder().add(CHOSEN_LANG, "nickName - " + teamName)).
                    add("abbreviation", Json.createObjectBuilder().add(CHOSEN_LANG, teamName.substring(0, 3))).
                    add("location", Json.createObjectBuilder().add(CHOSEN_LANG, location)).
                    add("isHome", JsonValue.TRUE).
                    add("organizations", Json.createArrayBuilder().add(generateSportsOrganization("American Football", "tivo:sp.41", false))).
                    add("altIds", Json.createArrayBuilder().add(generateAltId("ds:altTeamId", Optional.of("Alt." + id))).add(generateAltId("ds:altTeamId2", Optional.of("Alt2." + id))))
                    .build();
        }
        return team;
    }

    public JsonObject generateStation(String stationId, int id) {
        JsonObject station = Json.createObjectBuilder().add("type", "Station")
                .add("id", stationId + id).add("content", Json.createObjectBuilder().
                        add("altIds", Json.createArrayBuilder().add(generateAltId("tms:AltstationId", Optional.of("504393")))).
                        add("name", Json.createObjectBuilder().add(CHOSEN_LANG, "QA STATION " + stationId + id).build())
                        .add("callSign", "HISTE")
                        .add("id", stationId + id)

                        .build()).build();
        if (debug) jsonToString(station);
        return station;
    }

    public JsonObject generateAltId(String idField, Optional<String> idValue) {
        String value = idValue != null ? idValue.get() : UUID.randomUUID().toString();
        JsonObject altId = Json.createObjectBuilder().add("ns", idField).add("id", value).build();
        return altId;
    }

    //generate a top level program object
    public JsonObject generateProgram(String progId, String progTitle) {
        JsonObject program = Json.createObjectBuilder().
                add("type", "Program").
                add("id", "tms:prog.SH000" + progId).
                add("content", Json.createObjectBuilder().
                        add("id", "tms:prog.SH000" + progId).
                        add("altIds", Json.createArrayBuilder().add(generateAltId("tms:rootId", Optional.of("123212"))).add(generateAltId("ds:guid", Optional.of(UUID.randomUUID().toString())))).
                        add("title", Json.createObjectBuilder().add(CHOSEN_LANG, progTitle)).
                        add("credits", Json.createArrayBuilder().add(generateCredits("291019", CREDIT_NAMES[new Random().nextInt(CREDIT_NAMES.length - 1)], "HOST"))).
                        add("genres", Json.createArrayBuilder().add(generateGenres(202, "Special", "Special", false))).
                        add("releaseYear", 2017).
                        add("ratings", Json.createObjectBuilder().add("usTv", Json.createObjectBuilder().add("value", "TV14"))).
                        add("metadata", Json.createObjectBuilder().add("origNetwork", "NBC"))
                ).
                build();
        if (debug) jsonToString(program);
        return program;
    }

    //generate embeded credit object
    public JsonObject generateCredits(String personId, String name, String role) {
        JsonObject credit = Json.createObjectBuilder().
                add("personId", personId).
                add("name", name).
                add("role", role).build();
        return credit;
    }

    //Given an offerId, stationId, epWorkId, SeriesId, and startingDate, generate a linearOffer that falls within x minus/plus days of the current time
    public JsonObject generateLinearBlock(String offerId, String stationId, String epWorkId, String seriesId, int dateRange) {
        Map map = getStartEndTime(dateRange);
        int duration = Integer.parseInt(map.get("duration").toString());
        String startDateTimeUtc = map.get("startTime").toString();
        String endDateTimeUtc = map.get("endTime").toString();
        JsonObject linearBlock = Json.createObjectBuilder().add("type", "LinearBlock").add("id", offerId).add("expireTime", this.expireTime).
                add("content", Json.createObjectBuilder().
                        add("id", offerId).
                        add("altIds", Json.createArrayBuilder().add(generateAltId("Alt." + offerId, Optional.of(UUID.randomUUID().toString())))).
                        add("stationId", stationId).
                        add("date", "2016-12-31").
                        add("offers", Json.createArrayBuilder().add(Json.createObjectBuilder().
                                add("id", offerId).
                                add("workId", epWorkId).
                                add("seriesId", seriesId).
                                add("startTime", startDateTimeUtc).
                                add("endTime", endDateTimeUtc).
                                add("new", false).
                                add("live", false).
                                add("is3d", false).
                                add("hasDescriptiveAudio", false).
                                add("duration", duration * 60).
                                add("altIds", Json.createArrayBuilder().add(generateAltId("ds.altOffer", null)).
                                        add(generateAltId("tms.rootId", Optional.of("129321")))).
                                build()
                        ).build()).
                        build()).
                build();
        if (debug) jsonToString(linearBlock);
        return linearBlock;
    }

    public JsonObject generateOfferVod(String id, String epWorkId, String providerId, int dateRange) {
        Map map = getStartEndTime(dateRange);
        String startDateTimeUtc = map.get("startTime").toString();
        String endDateTimeUtc = map.get("endTime").toString();

        JsonObject vodOffer = Json.createObjectBuilder().
                add("type", "VodOffer").
                add("id", id).
                add("content", Json.createObjectBuilder().
                        add("id", id).
                        add("altIds", Json.createArrayBuilder().add(generateAltId("topVodProvider.offerId", Optional.of("1234")))).
                        add("license", "RENTAL").
                        add("workId", epWorkId).
                        add("startTime", startDateTimeUtc).
                        add("endTime", endDateTimeUtc).
                        add("providerId", "hulu.com").
                        add("providerAssetId", "AssetId" + providerId).
                        add("is3d", JsonValue.FALSE).
                        add("packageIds", Json.createArrayBuilder().add("svod:fbac8c81b55f9005f7ec09e32d29e40eb40")).
                        add("videoQuality", "HD").
                        add("screenFormat", "WIDESCREEN").
                        add("price", Json.createObjectBuilder().add("currency", "USD").add("value", 29900)).
                        add("ratings", Json.createObjectBuilder().add("isAdult", JsonValue.FALSE).add("mpaa", Json.createObjectBuilder().add("value", "PG")))
                ).
                build();
        if (debug) jsonToString(vodOffer);
        return vodOffer;
    }

    /*
    generate some Physical game offers to go with the VideoGame objects
     */
    public JsonObject generatePhysicalOffer(String id, String gameWorkId, String providerId, String seriesId, int dateRange) {
        Map map = getStartEndTime(dateRange);
        String startDateTimeUtc = map.get("startTime").toString();
        String endDateTimeUtc = map.get("endTime").toString();

        JsonObject physicalOffer = Json.createObjectBuilder().
                add("type", "PhysicalOffer").
                add("id", id).
                add("content", Json.createObjectBuilder().
                        add("id", id).
                        add("format", new String[]{"Disk", "Blue-Ray", "Online", "Floppy", "APP"}[new Random().nextInt(5)]).
                        add("seriesId", seriesId). // maybe we can attach it to a actual tv series?
                        add("altIds", Json.createArrayBuilder().add(generateAltId("topGameProvider.offerId", Optional.of("GTA1234" + providerId)))).
                        add("workId", gameWorkId).
                        add("startTime", startDateTimeUtc).
                        add("endTime", endDateTimeUtc)
                ).
                build();
        if (debug) jsonToString(physicalOffer);
        return physicalOffer;
    }

    //generate a plausible duration for each views within the date range into the future back to the past.
    public Map getStartEndTime(int dateRange) {
        Random random = new Random();
        int duration = CONTENT_DURATION[random.nextInt(CONTENT_DURATION.length)];
        long startTime, endTime = System.currentTimeMillis();
        int randomDate = (random.nextInt(dateRange * 2)) - dateRange;
        startTime = randomDate >= 0 ? (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(randomDate)) : (System.currentTimeMillis() - TimeUnit.DAYS.toMillis(Math.abs(randomDate)));
        endTime = startTime + TimeUnit.MINUTES.toMillis(duration);
        String startDateTimeUtc = new DateTime(startTime, DateTimeZone.forID("EST")).toString().split("\\.")[0] + "+00:00";
        String endDateTimeUtc = new DateTime(endTime, DateTimeZone.forID("EST")).toString().split("\\.")[0] + "+00:00";
        Map map = new HashMap();
        map.put("startTime", startDateTimeUtc);
        map.put("endTime", endDateTimeUtc);
        map.put("duration", duration);
        return map;
    }

    //top level asset object
    public JsonObject generateMovies(String movieId, String title, boolean addImage) {
        if (!addImage) return null;
        JsonObject movies = Json.createObjectBuilder().add("type", "Movie").
                add("id", movieId).
                add("expireTime", this.expireTime).
                add("content", Json.createObjectBuilder().add("id", movieId)
                        .add("title", Json.createObjectBuilder().add(CHOSEN_LANG, title)
                        )
                        .add("connectors", generateConnectors(new String[]{movieId, movieId + " other"}, "FRAN-" + movieId, title))
                        .add("genres", Json.createArrayBuilder().add(generateGenres(32, "SciFy", "Sci-Fi", false))).
                                add("images", Json.createArrayBuilder().add(addImage("tvbanners/v6/NowShowing/9181462/p9181462_blt_v6_aa.jpg", addImage)))).build();
        if (debug) jsonToString(movies);
        return movies;
    }

    //top level asset object
    public JsonObject generatePerson(String personId, String fullName) {
        JsonObject person = Json.createObjectBuilder().
                add("type", "Person").
                add("id", personId).
                add("content", Json.createObjectBuilder()
                        .add("id", personId)
                        .add("personName", Json.createObjectBuilder().
                                add("full", fullName).
                                add("given", fullName.split(" ")[0]).
                                add("family", fullName.split(" ")[1])
                        ).add("altIds", Json.createArrayBuilder().add(generateAltId("tms:personId", Optional.of("1012"))).add(generateAltId("tms:personId", Optional.of("1015"))))
                ).
                build();
        if (debug) jsonToString(person);
        return person;
    }

    /*generate a seriesId that is tied to a episodeId*/
    public JsonObject generateSeries(String seriesId, Optional<Boolean> addImg, String title) {
        boolean addImage = addImg.isPresent() ? addImg.get() : false;
        JsonObject seriesObject = Json.createObjectBuilder()
                .add("type", "Series")
                .add("expireTime", this.expireTime)
                .add("id", seriesId)
                .add("content", Json.createObjectBuilder()
                        .add("id", seriesId)
                        .add("altIds", Json.createArrayBuilder().add(generateAltId("tms:SeriesAltId", Optional.of("21112"))).add(generateAltId("tms:SeriesAltId3", Optional.of("31113"))))
                        .add("title", Json.createObjectBuilder()
                                .add(CHOSEN_LANG, "Series " + seriesId + ": " + title).build())
                        .add("description", Json.createObjectBuilder()
                                .add(CHOSEN_LANG, "Generated series description for " + seriesId + ": " + title).build())
                        .add("ratings", Json.createObjectBuilder()
                                .add("usTv", Json.createObjectBuilder()
                                        .add("value", new String[]{"TVMA", "TVY", "TVY7", "TVG", "TVPG", "TV14"}[new Random().nextInt(6)]).build()).build())
                        .add("genres", Json.createArrayBuilder().add(generateGenres(1, "Thriller", "altThriller", false)).add(generateGenres(2, "Suspense", "altSuspense", false)))
                        .add("qualityRatings", Json.createArrayBuilder().add(Json.createObjectBuilder().
                                add("provider", "youtube").add("rating", 0.30).build()).build())
                        .add("moods", Json.createObjectBuilder().add(CHOSEN_LANG, Json.createArrayBuilder().add("Dark").add("Gritty").add("Gripping")).
                                add("es", Json.createArrayBuilder().add("Oscuro").add("Arenoso").add("Emocionante")))
                        .add("keywords", Json.createObjectBuilder().add(CHOSEN_LANG, Json.createArrayBuilder().
                                add("Albuquerque, N.M").
                                add("House").
                                add("Laboratory")))
                        .add("credits", Json.createArrayBuilder().add(Json.createObjectBuilder().
                                add("personId", "10000" + seriesId).
                                add("name", CREDIT_NAMES[new Random().nextInt(CREDIT_NAMES.length - 1)]).
                                add("role", "ACTOR").
                                add("characterName", "Walter White")).
                                add(Json.createObjectBuilder().
                                        add("personId", "10002").
                                        add("name", "Vince Gilligan").
                                        add("role", "EXECUTIVE_PRODUCER")).
                                add(Json.createObjectBuilder().
                                        add("personId", "10003").
                                        add("name", "Mark Johnson").
                                        add("role", "DIRECTOR"))
                        )
                        .add("releaseYear", 2015)
                        .add("releaseDate", "2015-01-20")
                        .add("images", Json.createArrayBuilder().add(addImage("tvbanners/v6/NowShowing/9181462/p9181462_blt_v6_aa.jpg", addImage)))
                        .add("metadata", Json.createObjectBuilder().add("origNetwork", "AMC").add("website", "http:www.amctv.com/originals/breakingbad/"))
                        .build())
                .build();//final

        if (debug) jsonToString(seriesObject);
        return seriesObject;
    }

    public JsonObject generateMerchandise(String id, String name, String productType) {
        JsonObject merch = Json.createObjectBuilder().
                add("id", id).
                add("type", "Merchandise").
                add("content", Json.createObjectBuilder().
                        add("id", id).
                        add("altIds", Json.createArrayBuilder().add(generateAltId("ns", Optional.of("showId"))).add(generateAltId("id", Optional.of("CBY")))).
                        add("name", Json.createObjectBuilder().add(CHOSEN_LANG, name)).
                        add("productType", productType).
                        add("description", Json.createObjectBuilder().add("en", " Some general description about this Merchandise: " + name)).
                        add("categories", Json.createObjectBuilder().add(CHOSEN_LANG, Json.createArrayBuilder().add("Default Category || Apparel").add("Default Category||Apparel||Tops"))).
                        add("keywords", Json.createObjectBuilder().add(CHOSEN_LANG, Json.createArrayBuilder().add("T-shirt").add("cowboy bebop"))).
                        add("topCategory", "apparel").
                        add("releaseYear", 2017).
                        add("releaseDate", "2017-08-28")
                ).build();
        if (debug) jsonToString(merch);
        return merch;
    }

    //a game object is extended from CreativeWork -- when we create a creative work base object we should build this object in that way
    public JsonObject generateVideoGameObject(String id, String title, boolean addImage, long... optionalExpiration) {
        long gameExpiration = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(365); //expire a year from now
        if (optionalExpiration.length == 1) {
            gameExpiration = optionalExpiration[0];
        }
        JsonObject game = Json.createObjectBuilder().
                add("id", id).
                add("type", "VideoGame").
                add("expireTime", gameExpiration).
                add("content", Json.createObjectBuilder().
                        add("id", id).
                        add("altIds", Json.createArrayBuilder().add(generateAltId("tivoProgramId", Optional.of("TVidGameID" + id)))).
                        add("title", Json.createObjectBuilder().add(CHOSEN_LANG.toLowerCase(), title)).
                        add("platform", Json.createObjectBuilder().add("id", "PLAT" + id).add("name", "GenericPlatformName - " + title)). //name & id should be at least length 1 or null
                        add("publisher", Json.createObjectBuilder().add("name", "Generic Publisher - ETA Games " + id)). //Jira story say should be accessible by publisher.name (needs a name key to be correct)
                        add("developer", Json.createObjectBuilder().add("name", "Generic Developer - Studio Name ETA Games " + id)). //language agnostic, should be length 1 or null
                        add("description", Json.createObjectBuilder().add(CHOSEN_LANG.toLowerCase(), " Some general description about this VideoGame: " + title)).
                        add("keywords", Json.createObjectBuilder().add(CHOSEN_LANG.toLowerCase(), Json.createArrayBuilder().add("Airplanes").add("Piloting"))).
                        add("releaseYear", 2018).
                        add("releaseDate", "2017-08-28").
                        add("images", Json.createArrayBuilder().add(addImage("dvdboxart/v8/CllPhotos/4407/p4407_d_v8_aa.jpg", addImage))).
                        add("credits", Json.createArrayBuilder().add(generateCredits("317858" + id, CREDIT_NAMES[new Random().nextInt(CREDIT_NAMES.length - 1)], "ACTOR")).add(generateCredits("317855" + id + title, CREDIT_NAMES[new Random().nextInt(CREDIT_NAMES.length - 1)], "DIRECTOR"))))
                .build();
        //add some randomness to our data generation to create better test data -- null out platform and publisher
        JsonObject gameObNulls = game;
        if (new Random().nextBoolean()) {
            gameObNulls = jsonObjectToBuilder(game.getJsonObject("content")).
                    add("platform", JsonObject.NULL).
                    add("publisher", JsonObject.NULL).
                    add("connectors", generateConnectors(new String[]{id, "SH00TIVO" + new Random().nextInt(100) + "-" + CHOSEN_LANG, "other", "TVIDGAMEID" + new Random().nextInt(100) + "-" + CHOSEN_LANG}, "FRAN-" + id, title)).
                    add("developer", JsonObject.NULL).build();
            gameObNulls = jsonObjectToBuilder(game).add("content", gameObNulls).build();
        }
        if (debug) jsonToString(gameObNulls);
        return gameObNulls;
    }

    //ability to override json object with put like JSONObject
    //thanks stack-overflow https://stackoverflow.com/questions/26346060/javax-json-add-new-jsonnumber-to-existing-jsonobject
    private JsonObjectBuilder jsonObjectToBuilder(JsonObject jo) {
        JsonObjectBuilder job = Json.createObjectBuilder();
        for (Map.Entry<String, JsonValue> entry : jo.entrySet()) {
            job.add(entry.getKey(), entry.getValue());
        }
        return job;
    }

    public JsonObject generateTopLevelImages(String imageId, String assetId) {
        JsonObject topLevelImage = Json.createObjectBuilder().add("contentType", "image/jpg")
                .add("url", "tvbanners/v6/CllPhotos/334890/p334890_b_v6_aa.jpg")
                .add("qualifiers", Json.createArrayBuilder().add("CCNNER").build())
                .add("width", "120")
                .add("height", "180")
                .add("caption", Json.createObjectBuilder().add(CHOSEN_LANG, "Cutco Racing").build())
                .add("id", imageId)
                .add("assetId", assetId)
                .add("objectType", "Image")
                .build();
        jsonToString(topLevelImage);
        return topLevelImage;
    }

    public JsonObject addImage(String url, boolean addImage) {
        if (!addImage) return null;
        JsonObject image = Json.createObjectBuilder().add("contentType", "image/jpg").
                add("url", url).
                add("qualifiers", Json.createArrayBuilder().add("BOX_ART").build()).
                add("width", 360).
                add("height", 240).build();
        return image;
    }

    /*
     Call an external API to get a random title from http://www.setgetgo.com/randomword/get.php
     */
    public String getRandomTitle() throws IOException {
        StringBuilder result = new StringBuilder();
        String urlToRead = "http://www.setgetgo.com/randomword/get.php";
        URL url = new URL(urlToRead);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }

        int random = new Random().ints(1, langTitleSeeds.length).findFirst().getAsInt();

        String randomTitle = langTitleSeeds[random];

        if (result.toString().contains("Document Moved")) {
            langTitleSeeds = titleSeederFR;
            randomTitle += " Append Foreign " + langTitleSeeds[new Random().ints(1, langTitleSeeds.length).findAny().getAsInt()];
        } else randomTitle = langTitleSeeds[random] + " " + result.toString();

        //make title even longer with another level or hacked "random"
        langTitleSeeds = langMap.get(new Random().ints(1, langMap.size() + 1).findAny().getAsInt());
        randomTitle += " - " + langTitleSeeds[new Random().ints(1, langTitleSeeds.length).findAny().getAsInt()];

        return randomTitle + " " + new Random().ints(1960, 2020).findAny().getAsInt(); //append some random year to title
    }

    /*generate an episode that is tied to a seriesId*/
    public JsonObject generateEpisode(String epsId, Optional<Boolean> addImg, String title, String seriesId) throws Exception {
        boolean addImage = addImg.isPresent() ? addImg.get() : false;
        JsonObject episodeObject = Json.createObjectBuilder()
                .add("type", "Episode")
                .add("expireTime", this.expireTime)
                .add("id", epsId)
                .add("content", Json.createObjectBuilder()
                        .add("id", epsId)
                        .add("title", Json.createObjectBuilder()
                                .add(CHOSEN_LANG, title).build())
                        .add("episodeTitle", Json.createObjectBuilder()
                                .add(CHOSEN_LANG, "QA Episode Title -" + title).build())
                        .add("description", Json.createObjectBuilder()
                                .add(CHOSEN_LANG, "Generated description for " + title).build())
                        .add("seriesId", seriesId)
                        .add("images", Json.createArrayBuilder().add(addImage("dvdboxart/v8/CllPhotos/4407/p4407_d_v8_aa.jpg", addImage)))
                        .add("seasonId", JsonValue.NULL)
                        .add("genres", Json.createArrayBuilder().add(generateGenres(116431, "Drama", "Dramarada", false)))
                        .add("credits", Json.createArrayBuilder().add(generateCredits("317858" + seriesId, CREDIT_NAMES[new Random().nextInt(CREDIT_NAMES.length - 1)], "EXECUTIVE_PRODUCER"))
                                .add(generateCredits("317855", "Vince Gilligan", "ACTOR"))
                        ).
                                add("releaseYear", 2017).
                                add("releaseDate", "2017-01-28").
                                add("connectors", Json.createObjectBuilder().add("crossoverIds", Json.createArrayBuilder().add("SH000029950000")))
                        .build())
                .build();//final
        if (debug) jsonToString(episodeObject);
        return episodeObject;
    }

    /*generate a preview object for a given
    @param workId - an id of an episode, movie, or series
    */
    public JsonObject generatePreview(String prevId, String workId, String title) throws Exception {
        JsonObject previewObject = Json.createObjectBuilder()
                .add("type", "Preview")
                .add("expireTime", this.expireTime)
                .add("id", prevId)
                .add("content", Json.createObjectBuilder()
                        .add("id", prevId)
                        .add("title", Json.createObjectBuilder()
                                .add(CHOSEN_LANG, title).build())
                        .add("description", Json.createObjectBuilder()
                                .add(CHOSEN_LANG, "Generated description for " + title).build())
                        .add("workId", workId)).build();//final
        if (debug) jsonToString(previewObject);
        return previewObject;
    }

    /*
    Generate/Add connectors for movie objects //hardcode in at least 2 language
     */
    public JsonObject generateConnectors(String[] similarIds, String franchiseId, String franchiseTitle) {
        String franTitle = franchiseTitle == null ? "Star Wars" : franchiseTitle;
        JsonObject connectors = Json.createObjectBuilder().
                add("similarIds", Json.createArrayBuilder().
                        add(similarIds[0]).
                        add(similarIds[1])).
                add("franchises", Json.createArrayBuilder().add(Json.createObjectBuilder().
                        add("id", franchiseId).
                        add("name", Json.createObjectBuilder().
                                add(CHOSEN_LANG, "Star Wars").
                                add("en", "English - " + franTitle).
                                add("it", "Italians - " + franTitle)))).build();

        if (debug) jsonToString(connectors);
        return connectors;
    }

    /*
    Get system health check at 8889/healthcheck
    @param baseServerURL - url parameter for ffs i.e http://internal-app-network.net
    */
    public static JSONObject getSystemHealthCheck(String baseServerURL) throws IOException {
        StringBuilder healthStat = new StringBuilder();
        String requestURL = baseServerURL + ":8889/healthcheck";
        URL url = new URL(requestURL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            healthStat.append(line);
        }
        rd.close();
        JSONObject health = new JSONObject(healthStat.toString());
        System.out.println("\nDEADLOCK STATUS " + health.get("deadlocks").toString() + "\n\n");
        System.out.println("statsObject entire " + health.toString() + "\n");
        return health;
    }

    /*Convert a string already in json format back into JSONObject so it can be easily accessed by its key
     */
    public JSONObject convertToJSONObject(String data) throws Exception {
        JSONObject json = new JSONObject(data.toString());
        return json;
    }

    /* useless exercise of the JsonWriter object - you can just as easily do jsonObject.toString() and get the same result */
    public String jsonToString(JsonObject jsonObject) {
        StringWriter stringWriter = new StringWriter();
        JsonWriter writer = Json.createWriter(stringWriter);
        writer.writeObject(jsonObject);
        writer.close();
        System.out.println(stringWriter.getBuffer().toString());
        return stringWriter.getBuffer().toString();
    }


    /* create x (countPerType) amount each known catalog object and put it in a format ready for use by replacement or update call */
    public String printToString(int countPerType, String prefix) throws Exception {
        String pref = prefix + "-" + CHOSEN_LANG;
        StringBuilder docs = new StringBuilder();
        docs.append("[");
        for (int i = 1; i <= countPerType; i++) {
            setLangSeeder(CHOSEN_LANG.toUpperCase());
            String nativeTitle = langTitleSeeds[Math.abs(new Random().nextInt(langTitleSeeds.length - 1))];
            String title = nativeTitle;
            title += " " + getRandomTitle();
            if (this.createPreview) {
                docs.append(generatePreview("Prev" + i + pref, "MV" + suffix + i + "_" + pref, "Preview For Movie " + title) + ",");
            }
            docs.append(generateSportsOrganization("SportOrg-" + nativeTitle, "tivo:sp.41" + i + "_" + pref, true) + ","); //checked
            docs.append(generateSeries("SH00" + suffix + i + "-" + CHOSEN_LANG, Optional.<Boolean>of(true), title) + ","); //checked
            docs.append(generateSportsEvent("SP" + suffix + i + "_" + pref, "NFL Football -" + nativeTitle, i) + ","); //checked
            docs.append(generateEpisode("EP00" + suffix + i + "_" + pref, Optional.of(Boolean.TRUE), title, "SH00" + suffix + i + "-" + CHOSEN_LANG) + ","); //checked
            docs.append(generateStation("tivo:station:82660" + i + "_" + pref, i) + ","); //checked
            docs.append(generateVodCategory("tivo.vodCat" + suffix + i + "_" + pref, "Movies") + ","); //checked
            docs.append(generateCategory("tivo.cat" + suffix + i + "_" + pref, "root") + ","); //checked
            docs.append(generateVodPackage("tivo.vodPack" + suffix + i + "_" + pref, title, 30) + ","); //checked
            docs.append(generateOfferVod("vod.EP" + suffix + i + "_" + pref, "EP00" + suffix + i + "_" + pref, "hulu.com", 30) + ","); //checked
            docs.append(generateLinearBlock("lOfferEP" + suffix + i + "_" + pref, "tms:station:" + suffix + i + "_" + pref, "EP00" + suffix + i + "_" + pref, "SH00" + suffix + i + "_" + pref, 30) + ","); //checked
            docs.append(generateProgram("SH00" + suffix + i + "_" + pref, title) + ","); //checked
            docs.append(generateMovies("MV" + suffix + i + "_" + pref, title, true).toString() + ","); //checked
            docs.append(generateMerchandise("FUN-000" + i + "-" + CHOSEN_LANG, "T-Shirt " + title, "configurable") + ",");
            docs.append(generatePerson("person-" + suffix + i + "_" + pref, "Jack Reacher") + ","); //checked
            docs.append(generateChannelLineup(i + "_" + pref + "", "integrationTest" + i + "_" + pref) + ","); //checked
            docs.append(generateVideoGameObject("TVIDGAMEID" + i, "The Red Baron - ACE " + i + "-" + CHOSEN_LANG, true, System.currentTimeMillis() + TimeUnit.HOURS.toMillis(new Random().ints(1, 5).findAny().getAsInt())) + ","); //expire this between 1 and 5 hours from now
            docs.append(generatePhysicalOffer("TVIDGAMEID_OFFER" + i, "TVIDGAMEID" + i, "FAKE_GAME_PROVIDER", "SH00" + suffix + i + "-" + CHOSEN_LANG, 30) + ","); //checked
            docs.append(generateGenres(22, "Thriller - " + nativeTitle + "-" + CHOSEN_LANG, "Thriller", true) + (i == countPerType ? "" : ",")); //checked
            if ((i % 100) == 0) System.out.println(i);
        }
        docs.append("]");
        //there is no need to save in line when we have a fileWriter method already
        File output = new File("QAGeneratedCatalogData.json");
        FileWriter writer2 = new FileWriter(output);
        writer2.write(String.valueOf(docs));
        writer2.close();
        return docs.toString();
    }

    /*
    instead of posting everything at once in a contiguous series with printToString, we just generate post by the id
     */
    public String formatPostById(String id) throws Exception {
        String title;
        StringBuilder data = new StringBuilder();
        data.append("[");
        int int_id = 0;
        try {
            int_id = Integer.parseInt(id);
        } catch (NumberFormatException n) {
            //default the id to zero if it cannot be converted
        }

        title = getRandomTitle();
        data.append(generateSportsOrganization("Football", "tivo:sp.91" + id, true) + ",");
        data.append(generateSeries("SH00" + suffix + id, Optional.<Boolean>of(true), title) + ",");
        data.append(generateSportsEvent("tms:SP" + suffix + id, "NFL Football", int_id) + ",");
        data.append(generateEpisode("EP00" + suffix + id, Optional.of(Boolean.TRUE), title, "SH00" + suffix + id) + ",");
        data.append(generateStation("tms:station:82660" + id, int_id) + ",");
        data.append(generateVodCategory("tms.vodCat" + suffix + id, "Movies") + ",");
        data.append(generateCategory("tms.cat" + suffix + id, "root") + ",");
        data.append(generateVodPackage("tms.vodPack" + suffix + id, title, 30) + ",");
        data.append(generateOfferVod("vod.EP" + suffix + id, "EP00" + suffix + id, "hulu.com", 30) + ",");
        data.append(generateLinearBlock("lOfferEP" + suffix + id, "tms:station:" + suffix + id, "EP00" + suffix + id, "SH00" + suffix + id, 30) + ",");
        data.append(generateProgram("SH00" + suffix + id, title) + ",");
        data.append(generateMovies("MV" + suffix + id, title, true).toString() + ",");
        data.append(generatePerson("person-" + suffix + id, "Jack Reacher") + ",");
        data.append(generateChannelLineup(id + "", "integrationTest" + id) + ",");
        data.append(generateGenres(12, "Suspense", "Suspense", true));
        data.append("]");
        return data.toString();
    }


    //@todo should make this the generic method and clean up the other posts to use this method
    public String postEventToEndpoint(String jsonData, String postEndPoint,boolean...debug) throws Exception {
        String data = jsonData;
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        String status = "";
        try {
            HttpPost request = new HttpPost(postEndPoint);
            StringEntity params = new StringEntity(data, "UTF-8");
            request.addHeader("Authorization", "Basic "+this.catAuth);
            request.addHeader("content-type", "application/json");
            request.setEntity(params);

            HttpResponse response = httpClient.execute(request);
            status = response.getStatusLine().toString();

        } catch (Exception ex) {
            System.out.printf("could not post event to: %s due to %s\n", postEndPoint, ex.getMessage());
        } finally {
            httpClient.close();
            if(debug.length==0 || debug[0]==true){ // just default to print out status message unless explicitly set to false
                System.out.printf("Attempt to post events to %s completed with status %s\n ", postEndPoint, status);
            }
            return status;
        }
    }

    //put event to end point
    public String putEventToEndpoint(String jsonData, String putEndPoint,boolean...debug) throws Exception {
        String data = jsonData;
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        String status = "";
        try {
            HttpPut request = new HttpPut(putEndPoint);
            request.addHeader("Authorization", "Basic "+this.catAuth);
            request.addHeader("content-type", "application/json");
            StringEntity params = new StringEntity(data,"UTF-8");
            request.setEntity(params);

            HttpResponse response = httpClient.execute(request);
            status = response.getStatusLine().toString();

        } catch (Exception ex) {
            System.out.printf("could not PUT event to: %s due to %s\n", putEndPoint, ex.getMessage());
        } finally {
            httpClient.close();
            if(debug.length==0 || debug[0]==true){ // just default to print out status message unless explicitly set to false
                System.out.printf("Attempt to PUT events to %s completed with status %s\n ", putEndPoint, status);
            }
            return status;
        }
    }


    public String sendGetEventToEndPoint(String getEndpointURL) throws Exception {
        String response = null;
        try {
            URL url = new URL(getEndpointURL);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Authorization", "Basic "+this.catAuth);
            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                response = in.readLine();
                in.close();
                return response;
            } else return "{ bad response: "+responseCode+"}";
        } catch (Exception e) {
        }
        return response;
    }

    public JSONArray sendGetEventToEndPoint(String getEndpointURL, boolean returnJSON,boolean... setAdmin) throws Exception {
        JSONArray response = new JSONArray();
        boolean setAsAdmin = setAdmin.length==0?true:setAdmin[0];
        try {
            URL url = new URL(getEndpointURL);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            if(setAsAdmin==true){
                conn.setRequestProperty("Authorization", "Basic "+this.catAuth);
            }
            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String content = in.readLine();
                if (content.charAt(0) == '{') {
                    //this is not an array  -- convert it to array
                    response = new JSONArray("[" + content + "]");
                } else {
                    response = new JSONArray(in.readLine().toString()); //its already an array, convert it o JSONArray
                }
                in.close();
                return response;
            } else return response;

        } catch (Exception e) {
        }
        return response;
    }

    /* generate a new full replacement transaction id
     *@param catalogId - the catalog we want to create txid for
     *@param catServer - the catserverUrl*/
    public static String createReplacementTxid(String catalogId, String catServer) throws Exception {

        String catServer_local = catServer;
        boolean oldForm = catServer.split("catalogs").length <= 1; // its the form of http://localhost:8888/catalogs
        if (oldForm) {
            catServer_local = catServer != null ? catServer + "/" + catalogId + "/replace" : "http://localhost:8888/catalogs/sales-demo/replace";
        } else {
            catServer_local = catServer.replaceAll("\\bsales-demo\\b|\\block\\b", catalogId);
        }
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        String tid = null;
        try {
            HttpPost request = new HttpPost(catServer_local);
            StringEntity params = new StringEntity(catalogId);
            request.addHeader("Authorization", "Basic "+catAuth);
            request.addHeader("content-type", "application/json");
            request.setEntity(params);
            HttpResponse response = httpClient.execute(request);
            tid = EntityUtils.toString(response.getEntity());
            System.out.println(response.getStatusLine());
        } catch (Exception ex) {
            System.out.println("error: " + ex.getMessage());
        } finally {
            System.out.println("Transaction: " + tid);
            httpClient.close();
        }
        return tid;
    }

    public static String getReplacementTxid(String catalogId, String catServerReplace) {
        String txid = null;
        String requestURL = catServerReplace;
        boolean oldForm = catServerReplace.split("catalogs").length <= 1; // its the form of http://localhost:8888/catalogs
        if (oldForm) {
            requestURL = catServerReplace != null ? catServerReplace + "/" + catalogId + "/replace" : "http://localhost:8888/catalogs/" + catalogId + "/replace/";
        } else {
            requestURL = catServerReplace.replaceAll("\\bsales-demo\\b|\\block\\b", catalogId);
        }

        try {
            URL url = new URL(requestURL);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Authorization", "Basic "+catAuth);
            int responseCode = conn.getResponseCode();

            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                txid = in.readLine();
                in.close();
                return txid;
            } else return txid;

        } catch (Exception e) {
        }
        return txid;
    }

    /*
    Run a full replacement with a given data - the whole 9 yard of getting a transactionId and committing
    @param data - string of data already converted or read from file to
    catServerReplace = "http://internal-app-network.net:8888/catalogs
     */
    public static void postFullReplacementAndCommit(String data, String catServer, String catalogId) throws Exception {
        String transactionId = getReplacementTxid(catalogId, catServer);
        String replacementURL = catServer != null ? catServer : "http://localhost:8888/catalogs/sales-demo/replace/";
        replacementURL += transactionId;
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        try {
            HttpPost request = new HttpPost(replacementURL);
            StringEntity params = new StringEntity(data, "UTF-8");
            request.addHeader("Authorization", "Basic "+catAuth);
            request.addHeader("content-type", "application/json");
            request.setEntity(params);
            HttpResponse response = httpClient.execute(request);
            System.out.println("\n\n\nPosted Replacement " + response.getStatusLine());
        } catch (Exception ex) {
            System.out.println("error: " + ex.getMessage());
        } finally {
            httpClient.close();
        }
        commitTransaction(transactionId, catServer, "lock");
    }

    /*
   Appending full replacement data but don't commit until we call the commit transaction separately
   @param data - string of data already converted or read from file to
   catServerReplace = "http://internal-app-network.net:8888/catalogs
    */
    public static void postFullReplacement_NoCommit(String data, String catServer, String catalogId, String txid) throws Exception {

        String replacementURL = catServer;
        boolean oldForm = catServer.split("catalogs").length <= 1; // its the form of http://localhost:8888/catalogs
        if (oldForm) {
            replacementURL = catServer != null ? catServer + "/" + catalogId + "/replace/" + txid : "http://localhost:8888/catalogs/" + catalogId + "/replace/";
        } else {
            replacementURL = catServer.replaceAll("\\bsales-demo\\b|\\block\\b", catalogId) + "/" + txid;
        }
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        try {
            HttpPost request = new HttpPost(replacementURL);
            StringEntity params = new StringEntity(data, "UTF-8");
            request.setHeader("Authorization", "Basic "+catAuth);
            params.setContentType("application/json");
            request.setEntity(params);

            HttpResponse response = httpClient.execute(request);
            System.out.println("Posting Replacement Data -- Not Committing" + response.getStatusLine() + "\n");
            if (response.getStatusLine().toString().contains("Bad Request")) {
                System.out.println(" the content " + response.getEntity().getContent().toString());
            }
        } catch (Exception ex) {
            System.out.println("error: " + ex.getMessage());
        } finally {
            httpClient.close();
        }
    }


    public static void commitTransaction(String txid, String catServerReplace, String catalogId) throws Exception {

        String apiEndpoint = catServerReplace;
        boolean oldForm = catServerReplace.split("catalogs").length <= 1; // its the form of http://localhost:8888/catalogs
        if (oldForm) {
            apiEndpoint = catServerReplace != null ? catServerReplace + "/" + catalogId + "/replace/" + txid + "/commit" : "http://localhost:8888/catalogs/sales-demo/replace/" + txid + "/commit";
        } else {
            apiEndpoint = catServerReplace.replaceAll("\\bsales-demo\\b|\\block\\b", catalogId) + "/" + txid + "/commit";
        }

        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        try {
            HttpPost request = new HttpPost(apiEndpoint);
            request.addHeader("Authorization", "Basic "+catAuth);
            request.addHeader("content-type", "application/json");
            HttpResponse response = httpClient.execute(request);
            System.out.println("COMMITTING TRANSACTION response: " + response.getStatusLine());
        } catch (Exception ex) {
            System.out.println("error: " + ex.getMessage());
        } finally {
            httpClient.close();
        }
    }

    /*
    Call a roll back on a transactionId
    @param catServerReplace - the base url which to append txid to i.e http://internal-app-network.net:8888/catalogs/lock/replace/txid/rollback
     */
    public static void rollBackTransaction(String txid, String catServerReplace, String catalogId) {
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        String apiEndpoint = catServerReplace != null ? catServerReplace + "/" + catalogId + "/replace/" + txid + "/rollback" : "http://localhost:8888/catalogs/sales-demo/replace/" + txid + "/rollback";
        try {
            HttpPost request = new HttpPost(apiEndpoint);
            request.addHeader("Authorization", "Basic "+catAuth);
            request.addHeader("content-type", "application/json");
            HttpResponse response = httpClient.execute(request);
            System.out.println("TRANSACTION ROLLBACK response: " + response.getStatusLine());
        } catch (Exception ex) {
            System.out.println("error: " + ex.getMessage());
        } finally {
        }
    }


    /*
    Posting one doc at a time:
    instead of posting one large json doc, just post individual objects to catalog server via http request
    We are posting as updates rather than replacement so there is no need for transactionId,
    an example endpoint for this post is: catServerUpdate = "http://don-si-ec2-admin-0:8888/catalogs/lock/updates";
    */
    public void makeConsecutiveNormalUpdates(int countTimes, int lockaySec, String postEndpointUrl, int startingOffset) throws Exception {
        for (int i = 0; i < countTimes; i++) {
            System.out.println("\nPosting Normal Update For: " + i + ": " + formatPostById(startingOffset + String.valueOf(i)));
            postEventToEndpoint(formatPostById(startingOffset + String.valueOf(i)), postEndpointUrl != null ? postEndpointUrl : local_catServerUpdate);
            Thread.sleep(lockaySec * 1000);
        }
    }

    /*
    Use this to write json content generated into a file that we can use as test data later for manual testing
     */
    public static void saveCatalogContentToFile(String filename, String content) throws Exception {
        try (BufferedWriter bf = new BufferedWriter(new FileWriter(filename))) {
            bf.write(content);
            bf.flush(); //we need immediate writing without having to exit program
        } catch (Exception e) {
            System.out.println("Could not write content to file: " + e.getMessage());
        }
    }

    /*generate large tsv doc -- helper function for testing customer sales demo tool
     * @param countPerDoc - number of count for series and episode
     * @param filename - name of file to save the data to*/
    public void generateTSVdata(int docCount, String filename, String lang) throws Exception {
        StringBuilder tsvContent = new StringBuilder();
        String objectType[] = {"Series", "Episode"};
        String type = "";
        String content = "ID\tSeriesID\tTitle\tDescription\tObject Type\tLanguage\tGenre\tRelease Year\tCast\tImage URL\n";
        String seriesId = lang.equalsIgnoreCase(CHOSEN_LANG) ? "SH00201701" : "SH00201701" + lang;
        String episodeId = lang.equalsIgnoreCase(CHOSEN_LANG) ? "EP00201701" : "EP00201701" + lang;
        List<String> seriesList = new LinkedList<>();
        String title = "";
        String id = "";
        Set<String> genre = new HashSet<>();

        int genreCount = 1;
        tsvContent.append(content);
        for (int i = 0; i < docCount; i++) {
            genreCount = (new Random().nextInt(4) + 1); // randomly add between 1 and 3 genres
            for (int g = 0; g < genreCount; g++) {
                genre.add(genres[new Random().nextInt(genres.length)]);
            }
            String genreGroup = "";
            for (String s : genre) {
                genreGroup += s + ",";
            }
            genreGroup = genreGroup.substring(0, genreGroup.length() - 1);
            type = objectType[new Random().nextInt(2)];
            title = getRandomTitle();
            if (type.equalsIgnoreCase("Series")) {
                id = seriesId + lang + i;
                seriesList.add(id);
                content = id + "\t\t" + title + "\tA Generated description for " + title + "\t" + type + "\t" + lang + "\t" + genreGroup + "\t2017\tMichael C. Hall\thttp://www.dafont.com/forum/attach/orig/7/7/77419.png\n";
                tsvContent.append(content);
            } else {
                id = episodeId + lang + i;
                //add random episode to series connection -- the first episode will not have a real series
                seriesId = seriesList.size() > 0 ? seriesList.get((new Random().nextInt(seriesList.size()))) : seriesId + i;
                content = id + "\t" + seriesId + "\t" + title + "\tA Generated description for " + title + "\t" + type + "\t" + lang + "\t" + genreGroup + "\t2017\tMichael C. Hall\thttp://www.dafont.com/forum/attach/orig/7/7/77419.png\n";
                tsvContent.append(content);
            }
            seriesId = "SH00" + suffix + "01" + lang;
            if ((i % 100 == 0)) System.out.println(i);
            genre.clear();
        }
        try (BufferedWriter bf = new BufferedWriter(new FileWriter(filename))) {
            bf.write(tsvContent.toString());
        } catch (Exception e) {
            System.out.println("Could not write TSV file " + e.getMessage());
        }

    }


    /*get all docs from twc-assets*/
    public String getAssetChunks(String query, int limit, int startOffset, int endOffset, String assetSourceURL) throws Exception {
        File file = new File("qa-twc-assets.json");
        Writer writer = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
        String urlSource = assetSourceURL != null ? assetSourceURL :
                "http://qa-twc-staging-latest-admin-0.internal-app-network.net:8081/sd/twc/cores/assets-twc/search?wt=ffsjson&q=" + query +
                        "&fl=id,score,title,guid,releaseyear,description,genres,cast,teams,directors,contenttype,netflix,mpaaRating," +
                        "images.dvdboxart.v6,images.movieposters.v6,images.tvbanners.v6,images.URI,images.categories,images.height," +
                        "images.width,episodeInfo.title,sportsInfo.gameTime,sportsInfo.gameDate,relevantSchedules.prgSvcId," +
                        "relevantSchedules.prgSvc.name,relevantSchedules.provider,relevantSchedules.url,relevantSchedules.host," +
                        "relevantSchedules.viewingOptions,relevantSchedules.type,relevantSchedules.startTime,relevantSchedules.endTime," +
                        "rotten-tomatoes,common-sense,metacritic,sportsStats&warnings=true&fq=guid:*&" + "limit=" + limit + "&offset=" + startOffset;
        StringBuilder result = new StringBuilder();
        try {
            URL url = new URL(urlSource);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;

            while ((line = rd.readLine()) != null) {
                result.append(line.substring(line.indexOf("["), line.length() - 1));
            }
            rd.close();
        } catch (Exception ex) {
            System.out.println("GOT ERROR");
            return null;
        }

        writer.write(result.toString());
        writer.close();

        System.out.println("we wrote " + result.toString());

        return result.toString();
    }


    /*full replacement from a file - this calls the postFullReplacement which creates a transactionId and commit the string passed to it*/
    public static void postFullCatalogReplacementFromFile_AndCommit(String filename, String catServer, String catalogId) {
        StringBuilder content = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                content.append(line);
            }
            postFullReplacementAndCommit(content.toString(), catServer, catalogId);
        } catch (Exception e) {
            System.out.println("Got exception trying to read catalog contents to post " + e.getMessage());
        }
    }

    /*full replacement from a file - this calls the postFullReplacement which creates a transactionId - but don't commit yet*/
    public static void postFullCatalogReplacementFromFile_NoCommit(String filename, String catServer, String catalogId, String txid) {
        System.out.println("Let's just post from file so its faster ");
        StringBuilder content = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                content.append(line);
            }
            postFullReplacement_NoCommit(content.toString(), catServer, catalogId, txid);
        } catch (Exception e) {
            System.out.println("Got exception trying to read catalog contents to post " + e.getMessage());
        }
        System.out.println("End of Posting from file");
    }


    /*post docs to a normal assets core */
    public void postToNormalAssetsCore(String url, String fileName) throws Exception {
        String replacementURL = url != null ? url : "http://localhost:8080/ffs/qa-twc-assets/update?commitWithin=10000";
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        String data = convertFileContentToData(fileName);
        try {
            HttpPost request = new HttpPost(replacementURL);
            request.addHeader("content-type", "application/json");
            StringEntity params = new StringEntity(data, "UTF-8");
            request.setEntity(params);
            HttpResponse response = httpClient.execute(request);
            System.out.println("content" + response.getStatusLine());
            System.out.println("content" + EntityUtils.toString(response.getEntity()));
        } catch (Exception se) {
            System.out.println("Error while trying to write to core with data " + data + se.getMessage());
            System.out.println("Verify that the core is configured to for update");
        } finally {
            httpClient.close();
        }
    }


    /*store the json docs file into a string so it can be readily passed into http POSTs
     * this also so we do not have to read through them each time from the menu*/
    public static String convertFileContentToData(String filename) throws FileNotFoundException {
        StringBuilder content = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                content.append(line);
            }
        } catch (Exception e) {
            throw new FileNotFoundException(" - cannot find file to convert " + e.getMessage() + "\n\n");
        }
        return content.toString();
    }

    public static class RunnableAppend implements Runnable {
        private String threadName;
        private Thread t;
        //use hard-coded 'lock' catalog id. getTransactionId, if not found, create one
        private String txid = getReplacementTxid("lock", catServerReplace) == null ? createReplacementTxid("lock", catServerReplace) : getReplacementTxid("lock", catServerReplace);

        public RunnableAppend(String name) throws Exception {
            threadName = name;
            try {
                if (new Random().nextInt(3) == 1) getSystemHealthCheck(ffsBaseServer);
            } catch (Exception e) {
            }
        }

        @Override
        public void run() {

            System.out.println(System.currentTimeMillis() + " Running thread " + threadName + " to execute append calls with txid " + txid);
            try {
                postFullReplacement_NoCommit(default_data, catServerReplace, "lock", txid);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void start() {
            System.out.println("Starting append task at " + System.currentTimeMillis());
            if (t == null) {
                t = new Thread(this, threadName);
                t.start();
            }
        }
    }

    public static class RunnableRollback implements Runnable {
        private String threadName;
        private Thread t;
        private String txid = getReplacementTxid("lock", catServerReplace) == null ? createReplacementTxid("lock", catServerReplace) : getReplacementTxid("lock", catServerReplace);

        public RunnableRollback(String name) throws Exception {
            threadName = name;
            try {
                Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
            } catch (Exception e) {
            }
        }

        @Override
        public void run() {
            System.out.println(System.currentTimeMillis() + " Running thread " + threadName + " to execute rollback calls with txid " + txid);
            try {
                System.out.println("let thread sleep between 0-1s before calling a rollback");
                Thread.sleep(400);
                rollBackTransaction(getReplacementTxid("lock", catServerReplace), catServerReplace, "lock");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void start() {
            System.out.println("Starting rollback task at " + System.currentTimeMillis());
            if (t == null) {
                t = new Thread(this, threadName);
                t.start();
            }
        }
    }

    public static class RunnableCommit implements Runnable {
        private String threadName;
        private Thread t;
        private String txid = getReplacementTxid("lock", catServerReplace) == null ? createReplacementTxid("lock", catServerReplace) : getReplacementTxid("lock", catServerReplace);

        public RunnableCommit(String name) throws Exception {
            threadName = name;
            try {
                //   System.out.println("Instantiating thread"+name+" with txid "+txid);
            } catch (Exception e) {
            }
        }

        @Override
        public void run() {
            System.out.println(System.currentTimeMillis() + " Running thread " + threadName + " to execute commit calls with txid " + txid);
            try {
                getSystemHealthCheck(ffsBaseServer);
                Thread.sleep(2000); //sleep two seconds before committing
                commitTransaction(txid, catServerReplace, "lock");
                getSystemHealthCheck(ffsBaseServer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void start() {
            System.out.println("Starting rollback task at " + System.currentTimeMillis());
            if (t == null) {
                t = new Thread(this, threadName);
                t.start();
            }
        }
    }


    /*test for https://jira.corporate.local/browse/SD-10572 -- using threading we send in 5 appends & 2 rollback 'simultaneously'
    this setup recreates the catalog-deadlock issue found in catserver version 2.0.3.0 */
    public static void testConcurrentCatserverDeadlock() throws Exception {
        QAHelper cs = new QAHelper(LANG.EN, false, true, "QA-DEFAULT-DATA", 1716917900000L);
        RunnableAppend ra = new RunnableAppend("appendThread1");
        RunnableAppend ra4 = new RunnableAppend("appendThread4");
        RunnableAppend ra5 = new RunnableAppend("appendThread5");
        RunnableAppend ra2 = new RunnableAppend("appendThread2");
        RunnableAppend ra3 = new RunnableAppend("appendThread3");
        RunnableRollback rb = new RunnableRollback("rollbackThread");
        RunnableRollback rb2 = new RunnableRollback("rollbackThread2");
        RunnableCommit rc = new RunnableCommit("commit");
        rb.start();
        ra.start();
        ra2.start();
        ra3.start();
        ra4.start();
        ra5.start();
        rb2.start();
        rc.start();
        cs.getSystemHealthCheck(ffsBaseServer);
    }


    public static void main(String[] args) throws Exception {
        QAHelper cg = new QAHelper();
        cg.runMenu();
    }

    /*
    Method to verify that the User Events we posted to FFS resulted in meaningful interaction history - meaning
    ratings, likes, and purchases made it to the Voldelmort store and correctly return when the respective functions are called.
    Implicitly - this indicates that Kafka brokers are able to communicate with FFS, as FFS produces/emits these events to kafka topics
    @param eventSummary - json object (converted from our event generation report) that has info. about users and assets involved in a test
    @param baseSearchURL - the url to search against the assets and verify that the users now have likes, ratings, purchases etc
     */
    public boolean verifyHitsAndInteraction(JSONObject eventSummary, String baseSearchURL) throws Exception {
        String searchURL = baseSearchURL.substring(0, baseSearchURL.indexOf("id") + 3); //in case we had to read from file -- make it work both way

        System.out.println("the assets were " + eventSummary.get("ASSETS"));
        System.out.println("total users for test was " + eventSummary.get("totalUsers"));
        System.out.println("total users expected to pass in valid events were " + eventSummary.get("expectedTotalValidPosters"));
        System.out.println("total users expected to pass in bad events were " + eventSummary.get("expectedTotalInvalidPosters"));
        int successfulPosters = eventSummary.get("validInteractedUsers").toString().substring(1).replace("]", "").split(",").length;
        String invalidPosters[] = eventSummary.get("inValidInteractionsListUsers").toString().substring(1).replace("]", "").split(",");
        int invalidPostercount = invalidPosters.length;
        if (invalidPosters[0].isEmpty()) invalidPostercount = 0;
        JSONObject rest = null;
        //episode to --> array list of likes, ratings, and purchase
        List<String> likeEvents = new ArrayList<>();
        List<String> ratingEvents = new ArrayList<>();
        List<String> purchaseEvents = new ArrayList<>();
        List validPostersList = eventSummary.getJSONArray("validInteractedUsers").toList();

        JSONArray allAssetsEventHistory = new JSONArray(); // holds an array of all episodes, which in turn will contain all information about bad events against it
        JSONObject assetInteractionHistory = new JSONObject(); //holds one asset's event interactions for every user known to hit against it

        System.out.println("number of invalid Posts " + invalidPostercount);
        if (Integer.parseInt(eventSummary.get("expectedTotalValidPosters").toString()) == successfulPosters && Integer.parseInt(eventSummary.get("expectedTotalInvalidPosters").toString()) == invalidPostercount) {
            System.out.println("So far everything checks out:.. \n" +
                    "validInteractedUsers count == expectedTotalValidPosters\n" +
                    "inValidInteractionsListUsers == expectedTotalInvalidPosters\n");
        } else {
            System.out.println("Something is off here..");
            if (validPostersList.size() >= 1) {
                System.out.println("We got at least one posted events in -- let's see what's going on");
            } else {
                System.out.println("... Not a single valid post were found ");
                System.out.println("This is indicative of kafka producer error -- FFS was having problem producing to KAFKA");
                System.out.println("Check your events generation report file for more details");
                return false;
            }
        }
        /*all this is unnecessary, we can just assert that total validInteractedUsers == expectedTotalValidPosters
         * that would be enough to know whether something went wrong during POSTs, but for dramatic effect, we're going to execute search for every interacted user here
         * and see which ones they are and which events did not produce/emit a kafka message from them*/
        System.out.printf("Now we're going to search and verify correct users gets a likes, purchase, and rating against our assets %s \n", eventSummary.get("ASSETS"));
        List assets = eventSummary.getJSONArray("ASSETS").toList();


        //outer loops go through each title/asset
        for (int asset = 0; asset < assets.size(); asset++) {

            likeEvents = new ArrayList<>(); //reset for each new episodes
            purchaseEvents = new ArrayList<>(); // reset for each new episodes
            ratingEvents = new ArrayList<>(); //reset for each new episodes
            StringBuilder badLikes = new StringBuilder();
            badLikes.append("\"badLikes\":[");
            StringBuilder badPurchases = new StringBuilder();
            badPurchases.append("\"badPurchases\":[");
            StringBuilder badRatings = new StringBuilder();
            badRatings.append("\"badRatings\":[");

            //inner loop verify every valid users against each assets
            for (int user = 0; user < validPostersList.size(); user++) {
                //send get event for userx against asset n
                searchURL += assets.get(asset) + "&userId=" + validPostersList.get(user) + "&fl=id,likedByUser(),purchasedByUser(),ratedByUser()&enable.merchandising=false";
                try {
                    rest = new JSONObject(sendGetEventToEndPoint(searchURL));
                } catch (Exception e) {
                    System.out.println("\nSearching failed -- verify the core exists and try again");
                    return false;
                }

                Thread.sleep(new Random().nextInt(50));
                System.out.println("sent " + searchURL + "\n");

                JSONArray hits = new JSONArray(rest.get("hits").toString());

                //no point in checking validity if the asset doesn't even exist
                if (rest.getJSONArray("hits").toList().size() == 0) {
                    searchURL = baseSearchURL;
                    continue;
                }
                System.out.println(rest.get("queryId"));

                if (hasUser("purchasedByUser()", hits.getJSONObject(0))) {
                    System.out.println("hasPurchase!");
                } else {
                    purchaseEvents.add(validPostersList.get(user).toString());
                    badPurchases.append(validPostersList.get(user).toString() + appendComma(user, (validPostersList.size() - 1)));
                }

                if (hasUser("ratedByUser()", hits.getJSONObject(0))) {
                    System.out.println("Has ratings!");
                } else {
                    ratingEvents.add(validPostersList.get(user).toString());
                    badRatings.append(validPostersList.get(user).toString() + appendComma(user, (validPostersList.size() - 1)));
                }
                if (hasUser("likedByUser()", hits.getJSONObject(0))) {
                    System.out.println("Has likes!");
                } else {
                    likeEvents.add(validPostersList.get(user).toString());
                    badLikes.append(validPostersList.get(user).toString() + appendComma(user, (validPostersList.size() - 1)));
                }
                searchURL = baseSearchURL; //reset search url
            }
            badLikes.append("]");
            badRatings.append("]");
            badPurchases.append("]");
            String allEvents = "{" + assets.get(asset).toString() + ":{" + badLikes.toString() + "," + badRatings.toString() + "," + badPurchases.toString() + "}}"; //every user's interaction for this event
            assetInteractionHistory = new JSONObject(allEvents); // store this assets's user interactions that failed to validate with function calls
            allAssetsEventHistory.put(assetInteractionHistory); // store this episode's history in our asset history collection
        }

        //every single asset have valid hits against all three functions likesByUsers(),purchasedByUsers(),ratedByUsers()
        boolean allPass = confirmCleanAssetHistory(allAssetsEventHistory);
        if (!allPass) {
            String failReports = System.getProperty("user.dir") + "/tmp/fail_reports-" + System.currentTimeMillis() + ".json";
            System.out.printf("\nWe have some assets that did not get the proper value for against the tested functions - see the %s file\n", failReports.split("tmp")[1]);
            File file = new File(failReports);
            FileWriter out = new FileWriter(file);
            allAssetsEventHistory.put(new JSONObject("{baseURL:" + "\"" + searchURL.toString() + "\"" + "}")); //
            out.write(allAssetsEventHistory.toString());
            out.close();
        } else
            System.out.println("\nAll System Go Boss! -- Congratulations looks like our Kafka brokers, Stream-Jobs and Voldemort nodes are doing their job!\n");
        return allPass;
    }

    /*
    Method method to the verifyHitsAndInteraction - will record which users did not get the proper likes, ratings and purchase
    values etc. If everyone got it - then it is a pass.
     */
    public static boolean confirmCleanAssetHistory(JSONArray history) {
        boolean allPass = true;

        for (int i = 0; i < history.toList().size(); i++) {
            JSONObject asset = history.getJSONObject(i);
            int start = asset.toString().indexOf(":{") + 1;
            int end = asset.toString().length() - 1;
            System.out.println("Checking asset " + asset.toString().substring(1, start));
            String content = asset.toString().substring(start, end);
            JSONObject assetContent = new JSONObject(content);
            //its really sad, an array list from json object does not have a size() method -- so we have to split into
            String[] badLikes = assetContent.get("badLikes").toString().split(",");
            String[] badRatings = assetContent.get("badRatings").toString().split(",");
            String[] badPurchases = assetContent.get("badPurchases").toString().split(",");
            if (badLikes.length > 1 || badRatings.length > 1 || badPurchases.length > 1)
                return false; // as soon as we get one false, don't bother checking the rest -- something is broken
            else {
                if (badLikes[0].contains("[]") && badPurchases[0].contains("[]") && badRatings[0].contains("[]"))
                    allPass = true;
            }
        }

        return allPass;
    }

    //helper method to deal with adding commas to build up json objects
    public static String appendComma(int startOffset, int endOffset) {
        if (startOffset < endOffset) return ",";
        else return "";
    }

    /*
    A generic function to check whether and ffs function returns a value of 1
    @param evenType - the function we want to check i.e likedByUser()
    @param res - the hit JSON object of a particular item's search result
     */
    public static boolean hasUser(String eventType, JSONObject res) {
        return (res.get(eventType).toString().equals("1"));
    }


    public void printMenu() { System.out.println("\n The QAHelper is a tool to assist with common test scenarios ");
        System.out.print(" such as stream-jobs, catalog server and capture basic performance metrics.\n Select from your options below to begin\n\n");
        System.out.println("1 ) -  Create Catalog Object Test Data");
        System.out.println("2 ) -  Add Catalog Objects to Catserver to Update Standard Cores");
        System.out.println("3 ) -  Make A Full Replacement To Catalog");
        System.out.println("4 ) -  Add Test Data to Normal Core");
        System.out.println("5 ) -  Generate TSV Data For Sales-Demo Tool Testing");
        System.out.println("6 ) -  Simulate Thread Deadlock On Catserver");
        System.out.println("7 ) -  Test Kafka Brokers & Stream Jobs With Generated View Events");
        System.out.println("8 ) -  Verify ratings, purchases, views and likes for Producer/Consumer Test From Option 7");
        System.out.println("9 ) -  Explore large user-item-lists creation");
        System.out.println("10) -  Load test Voldemort user-itemlist Updating");
        System.out.println("11) -  Conduct Statistical Checks on Voldemort Load Test Data");
        System.out.println("12) -  Conduct Statistical Checks on Generic 2-Sample Experiments");
        System.out.println("14) -  Generate query events and conversions for SI Use Case Performance Testing");
        System.out.println("15) -  Run campaign and conversions for SI");
        System.out.println("16) -  Compare results from two URL");
        System.out.println("17) -  Compare kafka topic partition assignments from before and after upgrade");
        System.out.println("18) -  Replay queries from querylog avros");
        System.out.println("19) -  Replay view-events from view-events avros");
        System.out.println("20) -  Update user attributes from view-events avros");
        System.out.println("21) -  Generate static entitlement traffics");
        System.out.println("13) -  Exit Program\n");

    }

    private boolean showMenu = true;
    public static int appRunCounter = 0;

    public void runMenu() throws Exception {
        int choices = 0;
        while (showMenu) {
            printMenu();
            System.out.print("Select an option...");
            Scanner menuInput = new Scanner(System.in);
            try {
                choices = Integer.parseInt(menuInput.nextLine());
                executeChoices(choices);
                appRunCounter++;
            } catch (NumberFormatException nf) {
                System.out.println("Please try to enter a numeric value");
            }
        }
    }

    public void executeChoices(int option) throws Exception {
        Scanner scan;
        String content_data;
        switch (option) {
            case 1:
                System.out.println("\nWhat language would you like your data in? Choose from \n" +
                        "EN, ES, FR, DE, IT, PT_BR, KH, PT, PL, ZH, SV\n");
                Scanner langOpt = new Scanner(System.in);
                String langOption = langOpt.nextLine();
                if (!Arrays.asList(LANG.values()).toString().contains(langOption.toUpperCase())) {
                    System.out.println("Sorry that language option " + langOption.toUpperCase() + " is not supported yet!");
                } else {
                    System.out.print("How many counts of each different Catalog would you like?\n");
                    int catalogObjectCounts = 0;
                    try {
                        catalogObjectCounts = Integer.parseInt(new Scanner(System.in).nextLine());
                        System.out.println("You are about to get " + catalogObjectCounts + " counts of ");
                        System.out.println("SportsOrganization");
                        System.out.println("Series");
                        System.out.println("SportsEvent");
                        System.out.println("Episode");
                        System.out.println("Station");
                        System.out.println("VodCategory");
                        System.out.println("Category");
                        System.out.println("VodPackage");
                        System.out.println("OfferVod");
                        System.out.println("LinearBlock");
                        System.out.println("Program");
                        System.out.println("Movies");
                        System.out.println("Person");
                        System.out.println("ChannelLineup");
                        System.out.println("Merchandise");
                        System.out.println("Genres");
                        System.out.println("A total of " + (catalogObjectCounts * 18) + " Catalog Objects will be created for your test data"); //magic number :( 18 -- will change base on how many we actually generate
                        System.out.printf("Would you like to generate some Preview objects?... ");
                        scan = new Scanner(System.in);
                        this.createPreview = parseBooleanInput(scan.nextLine().toString());
                    } catch (NumberFormatException nf) {
                        System.out.println("Sorry not a valid number!");
                    }
                    System.out.print("Give a file name with the .json extension to where you want to save these catalog objects to..");
                    Scanner scanner = new Scanner(System.in);
                    String filePath = System.getProperty("user.dir") + "/tmp/" + scanner.nextLine();
                    CHOSEN_LANG = langOption.toLowerCase();
                    File file = new File(filePath);
                    FileWriter out = new FileWriter(file);
                    out.write(printToString(catalogObjectCounts, this.suffix));
                    out.close();
                    System.out.println("Please find the file in " + filePath);
                }
                break;
            case 2:
                System.out.print("Please provide a file from tmp directory i.e 'spanish.json' to update your Standard core...  ");
                scan = new Scanner(System.in);
                File data_file = new File(System.getProperty("user.dir") + "/tmp/" + scan.nextLine());

                try {
                    content_data = convertFileContentToData(data_file.getAbsolutePath());
                    if (content_data.length() > 5) {
                        System.out.println("Succeeded loading in test data \n");
                        System.out.print("Please provide the CatServer Update URL - defaults to " + local_catServerUpdate + "\n");//http://localhost:8888/catalog/demo/updates ...");local_catServerUpdate
                        String user_catServerUpdate = new Scanner(System.in).nextLine();
                        if (!user_catServerUpdate.contains("http")) {
                            System.out.println(user_catServerUpdate + "is not a valid url " + " defaulting to " + local_catServerUpdate);
                            user_catServerUpdate = local_catServerUpdate;
                        }
                        System.out.println("Posting update now to " + user_catServerUpdate);
                        postEventToEndpoint(content_data, user_catServerUpdate);
                    } else System.out.println("Failed to load in test sufficient test data");

                } catch (Exception e) {
                    System.out.printf("Please try another file %s", e.getMessage());
                }
                break;
            case 3:
                System.out.println("Ok...let's make a full replacement to existing catalog");
                System.out.print("First, what is the CS replace URL? Default is " + local_catServerReplace + "\n");
                String user_csReplaceURL = new Scanner(System.in).nextLine();
                String catalog = "sales-demo";
                if (!user_csReplaceURL.contains("http")) {
                    System.out.println(user_csReplaceURL + "is not a valid url " + " defaulting to " + local_catServerReplace);
                    user_csReplaceURL = local_catServerReplace;
                }
                catalog = user_csReplaceURL.split("catalogs")[1].split("/")[1];
                String txid = createReplacementTxid(catalog, user_csReplaceURL);
                txid = txid != null ? txid : getReplacementTxid(catalog, user_csReplaceURL);
                if (txid == null) {
                    System.out.println("Could not connect with your CatServer, please try again later..");
                } else {
                    if (txid.contains("already active")) {
                        System.out.println("Please commit or rollback existing active transaction before starting a new one");
                        System.out.println("Would you like to rollback existing transaction now?..");

                        if (parseBooleanInput(new Scanner(System.in).next()))
                            rollBackTransaction(getReplacementTxid(catalog, user_csReplaceURL), user_csReplaceURL.substring(0, user_csReplaceURL.indexOf("catalogs") + 8), catalog);
                        break;
                    }

                    System.out.printf("\nSuccessfully Created transactionId %s for full replacement\n", txid);
                    System.out.print("\nPlease provide file from tmp directory to replace existing catalog stream with..");
                    scan = new Scanner(System.in);
                    data_file = new File(System.getProperty("user.dir") + "/tmp/" + scan.nextLine());
                    System.out.println("\nOk.. we're going to update the standard cores from this file " + data_file.getAbsolutePath());
                    try {
                        content_data = convertFileContentToData(data_file.getAbsolutePath());
                        System.out.println("Ok replacing all catalog data for " + catalog + " from file " + data_file.getAbsolutePath());
                        postFullReplacement_NoCommit(content_data, user_csReplaceURL, catalog, txid);
                        commitTransaction(txid, user_csReplaceURL, catalog);
                    } catch (Exception e) {
                        System.out.printf("Full catalog replacement fail - please try again later %s \n", e);
                    }
                }
                break;
            case 4:
                System.out.println("Ok...Let's Add some more data to a regular core (non-standard catalog cores)");
                System.out.print("Please provide file from tmp directory i.e 'spanish.json'__");
                scan = new Scanner(System.in);
                data_file = new File(System.getProperty("user.dir") + "/tmp/" + scan.nextLine());
                System.out.println("Ok.. we're going to update the standard cores from this file " + data_file.getAbsolutePath());
                System.out.println("What the url to your normal core? Example: http://localhost:8080/ffs/qa-twc-assets/update?commitWithin=10000");
                System.out.print("Normal core url: ");
                scan = new Scanner(System.in);
                String normalCoreURL = scan.nextLine();
                if (!normalCoreURL.contains("http")) {
                    System.out.println(normalCoreURL + " is not a valid url " + " defaulting to ");
                    System.out.println("What the heck, you ask for it");
                }
                postToNormalAssetsCore(normalCoreURL, data_file.getAbsolutePath());
                break;
            case 5:
                System.out.println("Ok...Look that tool is deprecated.. if you want to generate data for it.. just call the generateTSVdata(docCount,'fileNamePath','sv')");
                break;
            case 6:
                System.out.println("Ah... You want to have fun eh?? Ok, let's generate simulate a simple deadlock traffic that affected CS v 2.0.4.0");
                testConcurrentCatserverDeadlock();
                break;
            case 7:
                System.out.println("We're going to send in some likes, ratings, purchases and view events to test kafka-broker");
                System.out.println("Please provide the url to the environment i.e https://qa-staging-flex-0.internal-app-network.net/sd/popName\n");
                String baseUrl = new Scanner(System.in).nextLine();
                if (!baseUrl.contains("http")) {
                    baseUrl = "https://don-si-ec2-flex-0.internal-app-network.net/sd/qa-offers";
                    System.out.printf("bad url (needs http) - giving you default %s", baseUrl);
                }
                baseSearchURL = baseUrl;
                System.out.println("\n Provide descriptive prefix for your users so you can easily remember i.e Broker_1_UpgradedUser\n");
                String userPrefix = new Scanner(System.in).nextLine();
                System.out.println("How many users do you want to generate events for?\n ");
                int totalUsers = Integer.parseInt(new Scanner(System.in).nextLine());
                System.out.printf("How many users do you want to send invalid events for out of the %d users?\n", totalUsers);
                int invalidUserCount = Integer.parseInt(new Scanner(System.in).nextLine());
                System.out.println("Ok, please provide a small list of episodeIds we will generate views for, separated by comas");
                String episodes[] = cleanUpUserArrayInput(new Scanner(System.in).nextLine().split(","));
                postEventsForUsers(userPrefix, totalUsers, episodes, invalidUserCount, baseUrl);
                break;
            case 8:
                System.out.println("8 - Verify ratings, purchases, views and likes for kafka broker tests");
                System.out.println("Enter the file name created from your events generation from option 7");
                String inFile = new Scanner(System.in).nextLine();
                try {
                    JSONObject eventsSummary = convertToJSONObject(convertFileContentToData(System.getProperty("user.dir") + "/tmp/" + inFile));
                    System.out.println("What is the search core for the population? __ ");
                    String searchCore = new Scanner(System.in).nextLine();
                    String tempURL = baseSearchURL;
                    baseSearchURL += "/cores/" + searchCore + "/search?q=id:";
                    if (!baseSearchURL.contains("http")) {
                        baseSearchURL = eventsSummary.getString("postEndpoint") + "/cores/" + searchCore + "/search?q=id:";
                    }
                    System.out.println("we're going to validate against " + baseSearchURL);
                    verifyHitsAndInteraction(eventsSummary, baseSearchURL);
                    baseSearchURL = tempURL; //reset the url back
                } catch (Exception e) {
                    System.out.printf("Please try with again %s\n\n", e);
                }
                break;
            case 9:
                System.out.println("Let's play with making user itemlists");
                System.out.println("How many lists do you want to create for the user?");
                int totalList = Integer.parseInt(new Scanner(System.in).nextLine());
                System.out.print("How many items should be in each list?\n");
                int itemsPerList = Integer.parseInt(new Scanner(System.in).nextLine());
                System.out.printf("Should the list size be fixed\n");
                boolean fixedSize = parseBooleanInput(new Scanner(System.in).nextLine());
                generateUserItemListArray(totalList, itemsPerList, fixedSize, true, null);
                break;
            case 10:
                System.out.println("Starting load test and tracking response time");
                System.out.println("Please provide user-itemlist update endpoint i.e http://qa-flex-0.internal-app-network.net/sd/qa-offers/users/mlisa/itemlists");
                String itemListEndpoint = new Scanner(System.in).nextLine();
                if (!itemListEndpoint.contains("http")) {
                    String randomUser = "userRandom-" + System.currentTimeMillis();
                    itemListEndpoint = "http://don-si-ec2-flex-0.internal-app-network.net/sd/qa-offers/users/" + randomUser + "/itemlists";
                    System.out.println("Invalid input given -- sending update to a default endpoint " + itemListEndpoint);
                }
                System.out.println("Please provide a name for the responseTime report with .json extension");
                String reportName = new Scanner(System.in).nextLine();
                System.out.println("reports will be written to tmp/" + reportName);
                startUserItemListAdd_UpdateLoadTest(itemListEndpoint, reportName);
                break;
            case 11:
                System.out.println("Please provide report data from the before upgrade run e.g statistics-beforeupgrade.json");
                String beforeUpgradeReport = new Scanner(System.in).nextLine();
                System.out.println("Please provide report data from the after upgrade run");
                String afterUpgradeReport = new Scanner(System.in).nextLine();
                System.out.println("...Starting analysis\n..");
                Thread.sleep(1000 * 3); //give a bit of work perception -- loading to quickly is a bit jaring
                try {
                    analyzeDifferencesInferPopulationStatistics(beforeUpgradeReport, afterUpgradeReport);
                } catch (FileNotFoundException e) {
                    System.out.printf("Please try another file %s", e.getMessage());
                }
                break;
            case 12:
                System.out.println("Please provide file containing sample1 and sample2 data -- see samples.json for data format example");
                String sampleFile = new Scanner(System.in).nextLine();
                System.out.println("...Starting one tail analysis\n..");
                Thread.sleep(1000 * 3); //give a bit of work perception -- loading to quickly is a bit jaring
                conductOneTailTest(sampleFile, 0.05, "Conducting test for generic sample1 & sample2 experiment");
                Thread.sleep(1000 * 5); //give user a bit of time to view results before loading next menu
                break;
            case 43:
                System.out.println("Generate some test query with random titles from catalog tivo demo core");
                System.out.println("How many queries?");
                int totalQueries = new Scanner(System.in).nextInt();
                System.out.println("How many total users? ");
                int totalU = new Scanner(System.in).nextInt();
                System.out.println("Begin generating test queries..");
                generateRandomQueriesForTivoNLU(totalQueries,totalU);
                break;
            case 13:
                System.out.println("Ok...Exiting Program");
                System.exit(1);
            case 42:
                System.out.println("Lets scrape some test data from a real core -- find assets that have linearOffers because they make interesting test data");
                int numberOfAssetsToScrape = 0;
                System.out.printf("Give a search tap url default will be %s\n", epsDataURL);
                String searchTap = new Scanner(System.in).nextLine();
                if (!searchTap.contains("http")) {
                    searchTap = epsDataURL;
                    System.out.printf("Invalid input given -- using default %s \n", searchTap);
                }

                try {
                    System.out.print("How many assets do you want to scrape for from the core?");
                    numberOfAssetsToScrape = Integer.parseInt(new Scanner(System.in).nextLine());
                } catch (Exception e) {
                    System.out.printf("invalid number provided -- defaulting to scraping %d\n", 1000);
                    numberOfAssetsToScrape = 1000;
                }

                System.out.print("Provide a file name to save data to i.e catalog-tivo-demo-assets-core.json");
                String fileName = new Scanner(System.in).nextLine();
                if (fileName.length() < 4) {
                    fileName = "catalog-tivo-demo-assets-core.json";
                    System.out.println("invalid file name provided, defaulting to catalog-tivo-demo-assets-core.json");
                }
                System.out.println("Creating test data ....");
                createRealLinearOffersTestData(searchTap, numberOfAssetsToScrape, 0, fileName);
                break;

            case 14:
                System.out.println("Generating query & event data for SI Product Use Case Performance ");
                System.out.println("Please provide the event configuration file i.e events-properties.json");
                String configFile = new Scanner(System.in).nextLine();
                try{
                    generateUseCasePerformanceData(configFile);
                }catch (FileNotFoundException f){
                    System.out.printf("Could not config file %s try again -- defaulting to UseCasePerformance.json ...\n",configFile);
                    System.out.println("Starting SI data generation");
                    generateUseCasePerformanceData("UseCasePerformance.json");
                }
                break;

            case 15:
                System.out.println("Run Campaign/Experiment simulation based on config ");
                System.out.println("Please provide the event configuration file i.e events-properties.json");
                String exp_configFile = new Scanner(System.in).nextLine();
                try{
                    runExperimentsTest(exp_configFile);
                }catch (FileNotFoundException f){
                    System.out.printf("Could not config file %s try again -- defaulting to UseCasePerformance.json ...\n",exp_configFile);
                    Thread.sleep(2000);
                    runExperimentsTest("UseCasePerformance.json");
                }

                break;
            case 16:
                System.out.println("Comparing search results between two URLS..");
                String outputFile = System.getProperty("user.dir")+"/tmp/"+"JSON_COMPARISON_"+System.currentTimeMillis()+".csv";
                File resultsFile = new File(outputFile);
                FileWriter fr = new FileWriter(resultsFile, true);
                BufferedWriter br = new BufferedWriter(fr);
                br.write("URL1௵URL2௵Pass (Same Response)௵Observed Differences\n");
                br.close();
                fr.close();
                System.out.println("Provide absolute path to file containing search URLS");
                String urlSearchFile = new Scanner(System.in).nextLine();
                System.out.println("Specify a delimeter between the two urls");
                String urlDelim = new Scanner(System.in).nextLine();
                JSONCompareMode mode = JSONCompareMode.STRICT; // use strict by default

                try {
                    compareEndPointSearchResults(new File(urlSearchFile),urlDelim,resultsFile,mode);
                    System.out.println("See the output at "+outputFile);
                } catch (FileNotFoundException e){
                    System.out.println("Cannot read file "+urlSearchFile+e.getStackTrace());
                }

                break;

            case 17:
                System.out.println("Provide kafka topic file from before i.e view-events.json");
                String beforeUpgradeTopic = new Scanner(System.in).nextLine();
                beforeUpgradeTopic = System.getProperty("user.dir")+"/tmp/"+beforeUpgradeTopic;
                System.out.println("Provide kafka topic file after kafka upgrades i.e view-events2.json");
                String afterUpgradeTopic =  new Scanner(System.in).nextLine();
                afterUpgradeTopic=System.getProperty("user.dir")+"/tmp/"+afterUpgradeTopic;

                String kafkaCompOutFile = System.getProperty("user.dir")+"/tmp/"+"KAFKA_PRODUCED_TOPIC__REPORT_"+System.currentTimeMillis()+".csv";
                File kafa_file_report = new File(kafkaCompOutFile);
                FileWriter fw = new FileWriter(kafa_file_report, true);
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write("Key ௵Before Meta௵After Meta௵Difference Description\n");
                bw.close();
                fw.close();
                compareKafkaTopicMeta(beforeUpgradeTopic,afterUpgradeTopic,kafa_file_report);
                break;

            case 18:
                System.out.println("Replaying queries from avro");
                System.out.println("Provide an absolute path for avro file or directory for querylogs");
                String queryLogPath = new Scanner(System.in).nextLine();
                System.out.println("Provide url to replay i.e https://qa-fios-staging2-admin-0.internal-app-network.net");
                String queryReplayURL =  new Scanner(System.in).nextLine();
                try {
                    EventQueryWork.replay_eventsFromAllDir(queryLogPath,this,queryReplayURL, EventQueryWork.AvroEventType.QUERYLOGS,false);
                } catch (FileNotFoundException e){
                    System.out.println("Invalid file or directory -- please try again");
                }
                break;

            case 19:
                System.out.println("Replaying view-events from avro");
                System.out.println("Provide an absolute path for avro file or directory for view-events");
                //all view events (can also replay from invalid-view-events path
                String viewLogPath = new Scanner(System.in).nextLine();
                System.out.println("Provide url to replay i.e https://qa-fios-staging2-admin-0.internal-app-network.net");
                String viewReplayURL =  new Scanner(System.in).nextLine();
                System.out.println("Replay event as today? T(yes)/F(no)");
                boolean replayAsToday = false;
                replayAsToday = parseBooleanInput(new Scanner(System.in).nextLine());
                try{
                    EventQueryWork.replay_eventsFromAllDir(viewLogPath,this,viewReplayURL, EventQueryWork.AvroEventType.VIEWS,replayAsToday);
                }catch (FileNotFoundException e){
                    System.out.println("Invalid file or directory -- please try again");
                }
                break;

            case 20:
                System.out.println("Updating user attributes from view-events avro");
                System.out.println("Provide an absolute path for avro file or directory for view-events to extract user from");
                String profileLogPath = new Scanner(System.in).nextLine();
                System.out.println("Provide url to replay i.e https://qa-fios-staging2-admin-0.internal-app-network.net");
                String profileReplayURL =  new Scanner(System.in).nextLine();
                try{
                    EventQueryWork.replay_eventsFromAllDir(profileLogPath,this,profileReplayURL,EventQueryWork.AvroEventType.USER_ATTRIBUTES,false);
                }catch (FileNotFoundException e){
                    System.out.println("Invalid file or directory -- please try again");
                }
                break;

            case 21:
                System.out.println("Generating static entitlement traffic");
                System.out.println("Please provide a popululation endpoint i.e http://qa-flex-0.internal-app-network.net/sd/tivo-nlu");
                String popURL = new Scanner(System.in).nextLine();
                System.out.println("How many users to update/add entitlements for?");
                int numberEntitlementUser = new Scanner(System.in).nextInt();
                if(popURL.isEmpty()) popURL="http://internal-app-network.net/sd/tivo-nlu";
                System.out.println("Provide a user prefix");
                String userPrefix_ = new Scanner(System.in).nextLine();
                if(userPrefix_.isEmpty()) userPrefix_ = generateLongUID();
                generateStaticEntitlementTrafic(userPrefix_,numberEntitlementUser,true,popURL);
                break;

            default:
                System.out.println("Please try again");
                break;
        }
    }

    /*test to compare json results of two different endpoints
    @Param url1 - the first url
    @Param url2 - the second url
    @Param file - the filename to append the results to
    @Param JSONCompareMode - LENIENT or Strict, lenient will ignore differences in array order results
     */
    private void compareJsonHits(String url1, String url2, File resultFile, JSONCompareMode mode) throws Exception {
        String data = url1 + "௵" + url2;
        FileWriter fr = new FileWriter(resultFile, true);
        BufferedWriter br = new BufferedWriter(fr);
        String resultType = "none";
        ObjectMapper dataMapper = null;
        JSONArray results1 = null;
        JSONArray results2 = null;
        String NA="N/A";

        try {

            String resp1 = sendGetEventToEndPoint(url1);
            String resp2 = sendGetEventToEndPoint(url2);

            try {
                results1 = new JSONArray("[" + (resp1) + "]");
                results2 = new JSONArray("[" + (resp2) + "]");
                //determine what type of result is returned i.e hits (regular search), terms (autoSuggest), or groups (composite queries or grouped results)
                resultType = results1.getJSONObject(0).keySet().toString().contains("hits") ? "hits" : results1.getJSONObject(0).keySet().toString().contains("terms") ? "terms" : results1.getJSONObject(0).keySet().toString().contains("groups") ? "groups" : results1.getJSONObject(0).keySet().toString().contains("item") ? "item" : results1.getJSONObject(0).keySet().toString().contains("facetCounts") ? "facetCounts" : "data";
                if (resultType.equalsIgnoreCase("item")) {
                    JSONObject rs1 = results1.getJSONObject(0).getJSONObject(resultType);
                    JSONObject rs2 = results2.getJSONObject(0).getJSONObject(resultType);
                    JSONAssert.assertEquals(rs1, rs2, mode);
                } else if(resultType.equalsIgnoreCase("data")){
                    JSONAssert.assertEquals(results1, results2, mode);
                    NA=results1.toString();
                } else{
                    results1 = results1.getJSONObject(0).getJSONArray(resultType);
                    results2 = results2.getJSONObject(0).getJSONArray(resultType);
                    JSONAssert.assertEquals(results1, results2, mode);
                }

            } catch (JSONException je) {
                //duplicate key exception -- happens on facetCounts the object allows multiple castName key with different values
                dataMapper = new ObjectMapper();
                JsonNode node1 = dataMapper.readTree(resp1);
                JsonNode node2 = dataMapper.readTree(resp2);
                Assert.assertEquals(node1.get("facetCounts"), node2.get("facetCounts"));
            } catch ( Exception res_e){
                System.out.println("Failed "+res_e.getMessage());
            }
            data = data.concat("௵Y௵"+NA + "\n");
            NA="N/A";
        } catch (AssertionError | JSONException e) {
            data = data.concat("௵N" + "௵" + e.getMessage().replaceAll("\n", "\t") + "\n");
        }
        br.write(data);
        br.close();
        fr.close();
    }

    /*
    Give a file with two different urls and some delimiter separating them, execute a search and compare their results
     */
    private void compareEndPointSearchResults(File urlFile, String url_separator, File resultFile, JSONCompareMode mode) throws Exception{
        String urls = null;
        try (BufferedReader br = new BufferedReader(new FileReader(urlFile))) {
            while ((urls = br.readLine()) != null) {
                compareJsonHits(urls.split(url_separator)[0],urls.split(url_separator)[1],resultFile,mode);
            }
        } catch (Exception e) {
            throw new FileNotFoundException("URL file cannot be found " + e.getMessage() + "\n\n");
        }

    }


    /*
    POSTs a series of add/update to sd/{pop}/{user}/itemlists and track the response time for statistical analysis
    A set of 1,000 unique list names will be posted for 100 time until 100,000 unique lists have been added for the user
    Then test updating a few item lists -- track response time for each updates
    https://www.unm.edu/~marcusj/2Sampletex1.pdf
     */
    private void startUserItemListAdd_UpdateLoadTest(String updateEndpoint, String outputFileName) throws Exception {
        JSONObject itemsToResponseTime = new JSONObject();
        List<JSONObject> uniqueUserLists = new ArrayList<>();
        itemsToResponseTime.put("0", 0);
        int itemsPerList = 100; //according to Jim the item list # is not important, only the # of unique lists and length of the list name
        int incrementer = 1000; //increment the number of list by 1000 after each subsequent post to see how they fair in linear fashion
        int totalExpectedUniqueList = 100000; //100,000 unique lists will be created at the end of the loop
        for (int i = 1; i <= 100; i++) {
            //create a thousand unique list each time, we should have 100k unique list quickly by the 100th iteration
            JSONArray data = generateUserItemListArray(incrementer, itemsPerList, true, false, i + "");
            //we are updating unique list
            itemsToResponseTime = postEventAndCaptureResponseTime(data, (incrementer * i) - incrementer, updateEndpoint, itemsToResponseTime); //(ic*i) == unique list at this point we are updating against
            uniqueUserLists.add(data.getJSONObject(0)); // get 100 unique list names from 100,000 for later use
        }

        //there should now be 100k unique lists in the voldemort store for this user, assert this to be confident about it
        Assert.assertTrue("Unique 100k itemlists of " + totalExpectedUniqueList + " have not been created yet", sendGetEventToEndPoint(updateEndpoint).split(",").length >= totalExpectedUniqueList);
        // want to see if there is a linear fashion in updates with increase number of list size when the unique list is fixed (1000k)
        JSONArray incremental_lists_update_array = new JSONArray();

        int numListCounter = 0;
        JSONObject incremental_lists_update_responseTime = new JSONObject();
        JSONObject last_Hundred_Single_UpdatesResponseTime = new JSONObject(); //store response time from single list updates after having fixed list (1000k) already in store

        System.out.println("We're doing single item list updates now after the 100,000th lists have been added");
        //we added least 100k list now -- let's cycle through some of our saved list and update them
        for (int uList = 0; uList < uniqueUserLists.size(); uList++) {

            //since we have built up 100k unique list, now lets check updating one list at a time, what kind of performance will we see?
            JSONObject itemList = uniqueUserLists.get(uList).put("itemIds", new JSONArray(new String[]{"item1"}));
            JSONArray data = new JSONArray();
            data.put(itemList);

            //x variable now is unchanged, so we'll just use the iteration number -- we only care about the response time deviation (y) -- should be pretty close to each other
            postEventAndCaptureResponseTime(data, (uList + 1), updateEndpoint, last_Hundred_Single_UpdatesResponseTime);

            //now check the response time increase (suspects increase in linear fashion) when we increase number of item lists to updates linearly
            for (int numList = 0; numList < uList; numList++) {
                itemList = uniqueUserLists.get(numList).put("itemIds", new JSONArray(new String[]{"newItemReplacement"}));
                incremental_lists_update_array.put(itemList);
                numListCounter++;
            }

            postEventAndCaptureResponseTime(incremental_lists_update_array, (numListCounter + 1), updateEndpoint, incremental_lists_update_responseTime);
            incremental_lists_update_array = new JSONArray(); //needs to reset
            numListCounter = 0; //reset num list count
        }

        //store all response summary to a json file which can be used for later analysis
        String user = updateEndpoint.split("users")[1].split("/")[1];
        JsonObject jsonObject = Json.createObjectBuilder().add("user", user).
                add("itemsToResponseTime", Json.createObjectBuilder().add("0", 0)).
                add("last_Hundred_Single_Updates", Json.createObjectBuilder().add("1", 0)).
                add("last_Hundred_NPlusOneLists_Updates", Json.createObjectBuilder().add("1", 0)).
                build();

        JSONObject JS = new JSONObject(jsonObject.toString());
        JS.put("itemsToResponseTime", itemsToResponseTime);
        JS.put("last_Hundred_Single_Updates", last_Hundred_Single_UpdatesResponseTime);
        JS.put("last_Hundred_NPlusOneLists_Updates", incremental_lists_update_responseTime);
        JS.put("uniqueUserLists", uniqueUserLists);

        File responseFile = new File(System.getProperty("user.dir") + "/tmp/" + outputFileName);
        FileWriter writer = new FileWriter(responseFile);
        writer.append(JS.toString());
        writer.close();
        System.out.printf("\nLoad test run complete.. writing response time data to %s file..%n", responseFile.getAbsolutePath());
        Thread.sleep(5000); //wait for file writing to catch up
    }

    /*
    capture response time and store it in x,y data format so it can be used for graphing or analysis
    @param dependentListVariable_X - can be number of lists of members/size of list - to correlate against response time
     */
    private JSONObject postEventAndCaptureResponseTime(JSONArray data, int dependentListVariable_X, String updateEndpoint, JSONObject itemsToResponseTime,boolean...debug) throws Exception {
        JSONObject respObject = itemsToResponseTime;
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        String status = "";
        try {
            HttpPost request = new HttpPost(updateEndpoint);
            StringEntity params = new StringEntity(data.toString());
            request.addHeader("Authorization", "Basic "+catAuth);
            request.addHeader("content-type", "application/json");
            request.setEntity(params);
            long startTime = System.currentTimeMillis();
            HttpResponse response = httpClient.execute(request);
            status = response.getStatusLine().toString();
            long duration = System.currentTimeMillis() - startTime;
            respObject.put(dependentListVariable_X + "", duration);
            //each time we have to update a list -- Voldemort must first read through all unique list -- this duration is time for reading existing list + adding in the new list
            System.out.printf("\nReading through %d unique list names before adding current list took %s ms\n", dependentListVariable_X, duration);
        } catch (Exception ex) {
            System.out.printf("\ncould not post event to: %s due to %s\n", updateEndpoint, ex.getMessage());
        } finally {
            httpClient.close();
            if(debug.length==0 || debug[0]==true){ // just default to print out status message unless explicitly set to false
                System.out.printf("\nAttempt to post events to %s completed with status %s \n", updateEndpoint, status);
            }
        }
        return respObject;
    }

    /*
    Generate an array of user item list for a single user - so it can be used for post against sd/{pop}/{user}/itemlists endpoint
    @param user - the user id to generate the list for
    @param numberOfLists - total number of list to create for this post body (ffs max to 100)
    @param itemsPerList - total number of item for each list (ffs max to 100,000)
    @param fixedItemSize - should the list member always be the same size each time we create a new list?
    @param listIncrementSize - if its not fixed size, how many items should we increment each list members by --- lets have fun with this test
     */
    public static JSONArray generateUserItemListArray(int numberOfLists, int itemsPerList, boolean fixedItemSize, boolean saveFile, String uniquePrefix) throws Exception {
        JSONArray userItemLists = new JSONArray();
        String prefix = uniquePrefix == null ? "" : uniquePrefix;
        int counter = 0;
        //outer loops to create the x numberOfLists
        List<String> items = new ArrayList<>();
        String uniqueListName = "";
        for (int i = 0; i < numberOfLists; i++) {
            uniqueListName = generateLongUID() + "_" + (i + 1) + "_" + prefix;
            //ever growing size for each list
            if (!fixedItemSize) {
                //keep adding more items by the itemsPerList
                for (int j = 0; j < itemsPerList; j++) {
                    counter++; //counter to help us increment itemIds
                    items.add("item-" + counter+"-"+generateLongUID());
                }
                //let's add this item to the list now

                JsonObject it = Json.createObjectBuilder().//(I use JsonObject instead of JSONObject because it is easier to build the object
                        add("name", uniqueListName).
                        add("description", "qa generated item" + (i + 1))
                        .build();
                JSONObject itemList = new JSONObject(it.toString()); //revert back to using JSONObject because it makes adding new keys easier in this case
                itemList.put("itemIds", new JSONArray(items));
                //now we put the complete list back into our POST body to collect all of them
                userItemLists.put(itemList);
            } else {
                //it's okay we just want to be happy with a small fixed size
                JsonObject it = Json.createObjectBuilder().//(I use JsonObject instead of JSONObject because it is easier to build the object
                        add("name", generateLongUID() + (i + 1)).
                        add("description", uniqueListName)
                        .build();
                JSONObject itemList = new JSONObject(it.toString()); //revert back to using JSONObject because it makes adding new keys easier in this case
                itemList.put("itemIds", new JSONArray(new String[]{"item1", "item2", "item3"}));
                userItemLists.put(itemList);
            }
        }
        //because saving is expensive
        if (saveFile) {
            System.out.print("Please give the list a name with json extension.. ");
            String fileName = System.getProperty("user.dir") + "/tmp/" + new Scanner(System.in).nextLine();
            BufferedWriter out = null;
            try {
                File itemListFile = new File(fileName);
                out = new BufferedWriter(new FileWriter(itemListFile));
                out.append(userItemLists.toString());
                out.flush();
                Thread.sleep(5000); // let writing catch up
                System.out.printf("Your list %s file has been created \n", fileName);
            } catch (Exception e) {
            } finally {
                out.close();
            }
        }

        return userItemLists;
    }

    public String[] cleanUpUserArrayInput(String[] input) {
        String temp[] = new String[input.length];
        for (int i = 0; i < input.length; i++) {
            temp[i] = input[i].replace('"', ' ').trim();
        }
        return temp;
    }

    /*
    Generate static PUT/ADD of entitlements for traffic test
    LinearSubs - Recordings - Purchase - VoDSubs
     */
    public void generateStaticEntitlementTrafic(String userPrefix,int numberOfUser,boolean includePost, String populationURL) throws Exception{
        long startTime = System.currentTimeMillis();
        String userId=userPrefix+"_TestEntitlement-userId";
        JSONObject resTime = new JSONObject();
        //generate static put/post environment for x number of users
        for(int i=0; i<numberOfUser; i++){
            if(includePost){
                postEventAndCaptureResponseTime(generateEntitlements(userId+i,"linear_post"),i,populationURL+"/linearSubscriptions",resTime,true);
                postEventAndCaptureResponseTime(generateEntitlements(userId+i,"vod_post"),i,populationURL+"/vodSubscriptions",resTime,true);
            }
            putEventToEndpoint(generateEntitlements(userId+i,"linear_put").toString(),populationURL+"/linearSubscriptions/"+userId+i,true);
            putEventToEndpoint(generateEntitlements(userId+i,"vod_put").toString(),populationURL+"/vodSubscriptions/"+userId+i,true);

            putEventToEndpoint(generateEntitlements(userId+i,"recording_put").toString(),populationURL+"/recordings/"+userId+i,true);
            putEventToEndpoint(generateEntitlements(userId+i,"purchase_put").toString(),populationURL+"/purchases/"+userId+i,true);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("POST Response time in ms "+resTime);
        System.out.println("Entitlement Update took: "+((endTime-startTime)/ 60000)+" minutes");
    }

    //return some static entitlement body object
    public JSONArray generateEntitlements(String userId, String type){
        switch (type) {
            case "linear_post":
                //POST /sd/mypop/linearSubscriptions
                JsonArray linearSubPOST = Json.createArrayBuilder().add(Json.createObjectBuilder().add("userId",userId).
                        add("subscriptions",Json.createArrayBuilder().
                                add(Json.createObjectBuilder().
                                        add("subscribedId","st001-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible").
                                        add("subscriptionType","STATION")).
                                add(Json.createObjectBuilder().
                                        add("subscribedId","lu001-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible").
                                        add("subscriptionType","LINEUP")).
                                add(Json.createObjectBuilder().
                                        add("subscribedId","st003-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible").
                                        add("subscriptionType","STATION").
                                        add("expireTime",System.currentTimeMillis()+TimeUnit.DAYS.toMillis(1))).
                                add(Json.createObjectBuilder().
                                        add("subscribedId","st004-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible").
                                        add("subscriptionType","STATION").
                                        add("expireTime",System.currentTimeMillis()+TimeUnit.DAYS.toMillis(1))).
                                add(Json.createObjectBuilder().
                                        add("subscribedId","st005-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible").
                                        add("subscriptionType","STATION").
                                        add("expireTime",System.currentTimeMillis()+TimeUnit.DAYS.toMillis(1)))

                        )).build();
                return  new JSONArray(linearSubPOST.toString());
            case "linear_put":
                ///sd/{population}/linearSubscriptions/{userId}
                JsonArray linearSub = Json.createArrayBuilder().add(Json.createObjectBuilder().
                        add("subscribedId","USA-CA04977-X").
                        add("subscriptionType","LINEUP").
                        add("expireTime",System.currentTimeMillis()+TimeUnit.DAYS.toMillis(1)).
                        add("profileId","QA")).build();
                return new JSONArray(linearSub.toString());
            case "vod_post":
                //POST /sd/{population}/vodSubscriptions
                JsonArray vodSubPOST = Json.createArrayBuilder().add(Json.createObjectBuilder().
                        add("userId",userId).
                        add("deviceId","qa-dev1-verylong-deviceIdName_to_see_compression_effects").
                        add("subscriptions",Json.createArrayBuilder().
                                add(Json.createObjectBuilder().
                                        add("providerId","netflix-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible").
                                        add("deviceId","qa-dev1-verylong-deviceIdName_to_see_compression_effects")).
                                add(Json.createObjectBuilder().
                                        add("providerId","amazon-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible").
                                        add("deviceId","qa-dev1-verylong-deviceIdName_to_see_compression_effects").
                                        add("expireTime",System.currentTimeMillis()+TimeUnit.DAYS.toMillis(1))).
                                add(Json.createObjectBuilder().
                                        add("providerId","amazon2-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible").
                                        add("deviceId","qa-dev1-verylong-deviceIdName_to_see_compression_effects").
                                        add("expireTime",System.currentTimeMillis()+TimeUnit.DAYS.toMillis(1))).
                                add(Json.createObjectBuilder().
                                        add("providerId","amazon3-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible").
                                        add("deviceId","qa-dev1-verylong-deviceIdName_to_see_compression_effects").
                                        add("expireTime",System.currentTimeMillis()+TimeUnit.DAYS.toMillis(1))).
                                add(Json.createObjectBuilder().
                                        add("providerId","amazon4-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible").
                                        add("deviceId","qa-dev1-verylong-deviceIdName_to_see_compression_effects").
                                        add("expireTime",System.currentTimeMillis()+TimeUnit.DAYS.toMillis(1)))
                        )).build();
                return new JSONArray(vodSubPOST.toString());
            case "vod_put":
                // PUT /sd/{population}/vodSubscriptions/{userId}
                JsonArray vodSub = Json.createArrayBuilder().add(Json.createObjectBuilder().add("providerId","hbo").
                        add("expireTime",System.currentTimeMillis()+TimeUnit.DAYS.toMillis(1)).
                        add("deviceId","qa-device-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible")).build();
                return new JSONArray(vodSub.toString());

            case "recording_put":
                // PUT /sd/{population}/recordings/{userId}¶
                JsonArray recordingSub = Json.createArrayBuilder().add(Json.createObjectBuilder().
                        add("id",userId+"-user-recording").
                        add("itemId","item1234").
                        add("deviceId","qa-device").
                        add("stationId","st-12345-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible").
                        add("timestamp",System.currentTimeMillis()).
                        add("duration",1800)).
                        add(Json.createObjectBuilder().
                                add("id",userId+"-user-recording-3").
                                add("itemId","item1234-qa-32").
                                add("deviceId","qa-device").
                                add("stationId","st-12345-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible-3").
                                add("timestamp",System.currentTimeMillis()).
                                add("duration",1800)).
                        build();
                return new JSONArray(recordingSub.toString());

            case "purchase_put":
                //PUT /sd/{population}/purchases/{userId}
                JsonArray purchaseSub = Json.createArrayBuilder().add(Json.createObjectBuilder().
                        add("id",userId).
                        add("itemId","item1234").
                        add("deviceId","qa-device").
                        add("providerId","provider-12345-make-this-a-very-long-provider-id-to-make-affects-of-compression-more-visible").
                        add("type","PURCHASE").
                        add("timestamp",System.currentTimeMillis()).
                        add("retailPrice",Json.createObjectBuilder().add("currency","USD").add("value",39900).build()).
                        add("purchasePrice",Json.createObjectBuilder().add("currency","USD").add("value",39900).build())).build();
                return new JSONArray(purchaseSub.toString());
                default:
                    return null;
        }

    }

    /*Random one time method to generate views, purchase, ratings & likes for N number of users
    Against a list of ids -- make it simple so we can easily verify the output in of stream-jobs
    and Voldemort updates -- summarize this simulation into a file
    */
    public JSONObject postEventsForUsers(String userPrefix, int numberOfUsers, String episodeIds[], int numberInvalidViews, String baseURL) throws Exception {
        System.out.println("Please enter a file name to save this run i.e brokerA_test.json: ");
        String fileName = new Scanner(System.in).nextLine();
        String startTime = new DateTime(System.currentTimeMillis(), DateTimeZone.getDefault()).toString();
        long startTimeEpoch = System.currentTimeMillis();
        String testInfo = "\"summary\":\"Every valid interaction were made against ratingEvents,purchaseEvents,viewEvents and ratingEvents endpoint - the invalid interaction were only POSTed against views " +
                "If Event Posting failed because FFS could nt produce to KAFKA then the inValidInteractionsListUsers will additionally add those unexpected failed users " +
                "and code --UFV for failed View Events, --UFP for failed Purchase Events, --UFL on failed Likes Event and --UFR for failed Ratings Events \"";
        StringBuilder summaryContent = new StringBuilder();
        summaryContent.append("{\n" + testInfo + ",\n");
        List<String> validUser = new ArrayList<>();
        Set<String> validUserSet = new HashSet<>();
        Set<String> inValidUserSet = new HashSet<>();
        List<String> invalidEventUsers = new ArrayList<>();

        List<Interaction> validInteractionsList = new ArrayList<>(); //a list that will hold an "interaction" object - which is an asset knows all interaction against it
        List<Interaction> inValidInteractionsList = new ArrayList<>(); // list of episodes that received invalid interaction either invalid record or server response error

        //post at events for x number of users against each episode
        for (int episode = 0; episode < episodeIds.length; episode++) {

            Interaction assetInteraction = new Interaction(episodeIds[episode]);
            Interaction inValidInteractionsListteraction = new Interaction(episodeIds[episode]);
            //give each users a like if they are within range for valid views 0 - (total users - invalidUser)
            for (int i = 0; i < numberOfUsers; i++) {
                if (i >= (numberOfUsers - numberInvalidViews)) {
                    //post an invalid endTime (365 days ago to generate some invalid views and checks for event metrics)
                    String data = generateViewEventData(userPrefix + i, episodeIds[episode], System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5), System.currentTimeMillis() - TimeUnit.DAYS.toMillis(365));
                    String status = postEventToEndpoint("[" + data + "]", baseURL + "/viewEvents");
                    if (!status.contains("202")) {
                        inValidInteractionsListteraction.addViewedUsers(userPrefix + i);
                        inValidUserSet.add(userPrefix + i);
                    } else {
                        System.out.println("Forget it man, this should never happen!");
                    }

                } else {
                    //put all the valid views here
                    String data = generateViewEventData(userPrefix + i, episodeIds[episode], System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5), System.currentTimeMillis());
                    String status = postEventToEndpoint("[" + data + "]", baseURL + "/viewEvents");
                    if (status.contains("202")) {
                        assetInteraction.addViewedUsers(userPrefix + i);
                        validUserSet.add(userPrefix + i);
                    } else {
                        System.out.println("something is wrong -- this is suppose to pass");
                        invalidEventUsers.add("--UFV" + userPrefix + i);
                        inValidInteractionsListteraction.addViewedUsers("--UFV" + userPrefix + i); //unexpected
                    }


                    data = generatePurchaseEvents(userPrefix + i, episodeIds[episode], System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5), System.currentTimeMillis());
                    status = postEventToEndpoint("[" + data + "]", baseURL + "/purchaseEvents");
                    if (status.contains("202")) {
                        validUserSet.add(userPrefix + i);
                        assetInteraction.addPurchasedUsers(userPrefix + i);
                    } else {
                        System.out.println("something is wrong -- this is suppose to pass");
                        invalidEventUsers.add("--UFP" + userPrefix + i);
                        inValidInteractionsListteraction.addPurchasedUsers("--UFP" + userPrefix + i); //unexpected
                    }


                    data = generateRatingEvents(userPrefix + i, episodeIds[episode], System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5), System.currentTimeMillis());
                    status = postEventToEndpoint("[" + data + "]", baseURL + "/ratingEvents");
                    if (status.contains("202")) {
                        validUserSet.add(userPrefix + i);
                        assetInteraction.addRatedUsers(userPrefix + i);
                    } else {
                        System.out.println("something is wrong -- this is suppose to pass");
                        invalidEventUsers.add("--UFR" + userPrefix + i);
                        inValidInteractionsListteraction.addRatedUsers("--UFR" + userPrefix + i); //unexpected
                    }


                    data = generateLikeEventData(userPrefix + i, episodeIds[episode], System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5), System.currentTimeMillis());
                    status = postEventToEndpoint("[" + data + "]", baseURL + "/likeEvents");
                    if (status.contains("202")) {
                        validUserSet.add(userPrefix + i);
                        assetInteraction.addLikedUsers(userPrefix + i);
                    } else {
                        System.out.println("something is wrong -- this is suppose to pass");
                        invalidEventUsers.add("--UFL" + userPrefix + i);
                        inValidInteractionsListteraction.addViewedUsers("--UFL" + userPrefix + i); //unexpected
                    }

                }
            }
            validInteractionsList.add(assetInteraction);
            inValidInteractionsList.add(inValidInteractionsListteraction);
        }

        StringBuilder validInteractions = new StringBuilder();

        validInteractions.append("\"ASSETS\":[");
        for (int i = 0; i < validInteractionsList.size(); i++) {
            validInteractions.append("\"" + validInteractionsList.get(i).getInteractionAssetId() + "\"");
            if (i != validInteractionsList.size() - 1) validInteractions.append(",");
        }
        validInteractions.append("]");

        StringBuilder invalidInteractions = new StringBuilder();
        for (Interaction invalidI : inValidInteractionsList) {
            System.out.printf("\nThese users %s sent invalid views against these assets:\n %s\n", invalidI.getViewUsers().toString(), invalidI.getInteractionAssetId());
        }
        validUser.addAll(validUserSet);
        invalidEventUsers.addAll(inValidUserSet);
        String postEndTime = new DateTime(System.currentTimeMillis(), DateTimeZone.getDefault()).toString();
        long endTimeEpoch = System.currentTimeMillis();
        long duration = (endTimeEpoch - startTimeEpoch) / 60000; //convert to minutes
        summaryContent.append("\"validInteractedUsers\":" + doubleQuoteListValues(validUser) + ",\n");
        summaryContent.append("\"inValidInteractionsListUsers\": " + doubleQuoteListValues(invalidEventUsers) + ",\n");
        summaryContent.append(("\"postStartTime\":\"" + startTime + "\"") + ",\n");
        summaryContent.append("\"postEndTime\":\"" + postEndTime + "\"" + ",\n");
        summaryContent.append(validInteractions + ",\n");
        summaryContent.append("\"totalUsers\":" + numberOfUsers + ",\n");
        summaryContent.append("\"expectedTotalValidPosters\":" + (new Integer(numberOfUsers - numberInvalidViews)) + ",\n");
        summaryContent.append("\"expectedTotalInvalidPosters\":" + numberInvalidViews + ",\n");
        summaryContent.append("\"totalElapsedTime_MINUTE\":" + duration + ",\n");
        summaryContent.append("\"postEndpoint\":\"" + baseURL + "\"" + "\n");
        summaryContent.append("}");
        File eventsFile = new File(System.getProperty("user.dir") + "/tmp/" + fileName);
        FileWriter writer = new FileWriter(eventsFile);
        writer.write(String.valueOf(summaryContent));
        writer.close();
        Thread.sleep(2000); //sleep for two seconds let the creation of the file get caught up before opening
        String data = convertFileContentToData("tmp/" + fileName);
        System.out.printf("\nGenerated views report have been written to %s\n", eventsFile.getAbsolutePath());
        return convertToJSONObject(data);
    }

    public JsonArray generateBaseEventData(String userId, String itemId, long timeStamp) {
        JsonArray baseEventData = Json.createArrayBuilder().add(Json.createObjectBuilder().
                add("itemId", itemId).
                add("userId", userId).
                add("timestamp", timeStamp).
                add("utcOffset", "-0400")).build();
        return baseEventData;
    }

    //this is getting a bit absurd
    public String doubleQuoteListValues(List<String> list) {
        List<String> newList = new ArrayList<>();
        for (String s : list) {
            newList.add("\"" + s + "\"");
        }
        return newList.toString();
    }

    public String generateViewEventData(String userId, String itemId, long startTime, long endTime, HashMap...args) throws Exception {
        JsonArray baseData = generateBaseEventData(userId, itemId, startTime);
        JSONObject viewData = convertToJSONObject(baseData.getJsonObject(0).toString());
        viewData.put("startTime", startTime);
        viewData.put("endTime", endTime);
        viewData.put("startOffset",0);
        viewData.put("endOffset",TimeUnit.MILLISECONDS.toSeconds(endTime-startTime));
        viewData.put("type", "LINEAR");

        for(Map m: args){
            viewData.put("stationId",m.get("stationId"));
            viewData.put("viewDuration",Long.parseLong(m.get("duration").toString()));
            if(m.get("queryId")!=null){
                viewData.put("queryId",m.get("queryId"));
            }
        }

        return viewData.toString();
    }

    public String generateLikeEventData(String userId, String itemId, long startTime, long endTime) throws Exception {
        JsonArray baseData = generateBaseEventData(userId, itemId, startTime);
        JSONObject viewData = convertToJSONObject(baseData.getJsonObject(0).toString());
        viewData.put("operation", "LIKE");
        return viewData.toString();
    }

    public String generateRatingEvents(String userId, String itemId, long startTime, long endTime) throws Exception {
        JsonArray baseData = generateBaseEventData(userId, itemId, startTime);
        JSONObject viewData = convertToJSONObject(baseData.getJsonObject(0).toString());
        viewData.put("rating", 4);
        return viewData.toString();
    }

    public String generatePurchaseEvents(String userId, String itemId, long startTime, long endTime) throws Exception {
        JsonArray baseData = generateBaseEventData(userId, itemId, startTime);
        JSONObject viewData = convertToJSONObject(baseData.getJsonObject(0).toString());
        viewData.put("type", "PURCHASE");
        viewData.put("retailPrice", 2.99);
        viewData.put("purchasePrice", 2.99);
        return viewData.toString();
    }

    public String generateImplicitViewEvents(String userId, String itemId, long timestamp,String deviceType) throws Exception {
        JsonArray baseData = generateBaseEventData(userId,itemId,timestamp);
        JSONObject implictData = convertToJSONObject(baseData.getJsonObject(0).toString());
        implictData.put("deviceType",deviceType);
        implictData.put("type","VIEW_DETAILS"); //defaults to a click-through type
        return implictData.toString();
    }

    //convert epochToLocalDateTime for our sakes
    public String getLocalDateTime(long epochTime) throws Exception {
        DateTime localDate = new DateTime(epochTime, DateTimeZone.getDefault()); //defaults will use user's time zone
        return String.valueOf(localDate);
    }

    /*
    Reads an avro file with a generic schema or specified schema if one is given
    @param schemaFile the schema avsc file
    @param avroFile the .avro file
     */
    public DataFileReader<GenericRecord> readAvroFile(String schemaFile, String avroFile) throws Exception {
        Schema schema;
        DatumReader<GenericRecord> datumReader;
        if (schemaFile != null) {
            schema = new Schema.Parser().parse(new File(schemaFile));
            datumReader = new GenericDatumReader<GenericRecord>(schema);
        } else {
            datumReader = new GenericDatumReader<GenericRecord>(); //use default schema from the avro file's header
        }
        File file = new File(avroFile);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        return dataFileReader;
    }

    public enum interactionTypes {LIKES, RATINGS, PURCHASES, VIEWS}

    //Object to hold a single's asset's interaction
    class Interaction {
        private String interactionAssetId;
        private List<String> likesUsers = new ArrayList<>();
        private List<String> viewsUsers = new ArrayList<>();
        private List<String> purchaseUsers = new ArrayList<>();
        private List<String> ratingUsers = new ArrayList<>();
        private String userId;

        Interaction(String itemId, String userId) {
            this.interactionAssetId = itemId;
            this.userId = userId;
        }


        public String getInteractionAssetId() {
            return this.interactionAssetId;
        }

        Interaction(String itemId) {
            this.interactionAssetId = itemId;
        }

        public void addLikedUsers(String userId) {
            this.likesUsers.add(userId);
        }

        public List<String> getLikedUsers() {
            return likesUsers;
        }

        public void addViewedUsers(String userId) {
            viewsUsers.add(userId);
        }

        public List<String> getViewUsers() {
            return viewsUsers;
        }

        public void addPurchasedUsers(String userId) {
            this.purchaseUsers.add(userId);
        }

        public List<String> getPurchaseUsers() {
            return purchaseUsers;
        }

        public void addRatedUsers(String userId) {
            this.ratingUsers.add(userId);
        }

        public List<String> getRatedUsers() {
            return ratingUsers;
        }
    }

    public static String generateLongUID() throws Exception {
        return "Long_Unique_ID_ShouldNotBeDoneTrivially_" + UUID.randomUUID().toString() + "__BC_QA_DO_NOT_APPROVE";
    }

    /*
    Make some inferences about the samples
     */
    public void analyzeDifferencesInferPopulationStatistics(String beforeResponseTimeFile, String afterResponseTimeFile) throws Exception {
        String data = convertFileContentToData(System.getProperty("user.dir") + "/tmp/" + beforeResponseTimeFile);
        String afterData = convertFileContentToData(System.getProperty("user.dir") + "/tmp/" + afterResponseTimeFile);
        JSONObject beforeDataObject = new JSONObject(data);
        JSONObject afterDataObject = new JSONObject(afterData);

        //use before to denote first sample set and after to denote second sample e.g post upgrade results
        DescriptiveStatistics statsLast100_NPlusOneListUpdatesBefore = new DescriptiveStatistics();
        DescriptiveStatistics statsLast100_NPlusOneListUpdatesAfter = new DescriptiveStatistics();

        DescriptiveStatistics statsLastHundred_SingleUpdatesBefore = new DescriptiveStatistics();
        DescriptiveStatistics statsLastHundred_SingleUpdatesAfter = new DescriptiveStatistics();


        for (int i = 1; i <= 100; i++) {
            statsLastHundred_SingleUpdatesBefore.addValue(beforeDataObject.getJSONObject("last_Hundred_Single_Updates").getDouble(String.valueOf(i)));
            statsLastHundred_SingleUpdatesAfter.addValue(afterDataObject.getJSONObject("last_Hundred_Single_Updates").getDouble(String.valueOf(i)));

            statsLast100_NPlusOneListUpdatesBefore.addValue(beforeDataObject.getJSONObject("last_Hundred_NPlusOneLists_Updates").getDouble(String.valueOf(i)));
            statsLast100_NPlusOneListUpdatesAfter.addValue(afterDataObject.getJSONObject("last_Hundred_NPlusOneLists_Updates").getDouble(String.valueOf(i)));
        }

        //conduct pairwiset test if its the same user running updates against the same set of unique lists
        if (beforeDataObject.getString("user").equalsIgnoreCase(afterDataObject.getString("user"))) {
            conductPairwiseTTest(statsLastHundred_SingleUpdatesBefore.getSortedValues(), statsLastHundred_SingleUpdatesAfter.getSortedValues());
        }

        //test for last100_NPlusone
        conductOneTailTest(statsLast100_NPlusOneListUpdatesBefore.getSortedValues(), statsLast100_NPlusOneListUpdatesAfter.getSortedValues(), 0.05, "To Reject Null Hypothesis that the performance remained unchanged for incrementally updating n+1 itemlists up to 100");

        //test for last100_Single updates
        conductOneTailTest(statsLastHundred_SingleUpdatesBefore.getSortedValues(), statsLastHundred_SingleUpdatesAfter.getSortedValues(), 0.05, "To Reject Null Hypothesis that Update after 100k unique item lists in voldemort still have the same response time performance");

        /* same deal here -- just trying to see if I can save heap to run the while loop below */
        statsLast100_NPlusOneListUpdatesAfter.clear();
        statsLast100_NPlusOneListUpdatesBefore.clear();
        statsLastHundred_SingleUpdatesAfter.clear();
        statsLastHundred_SingleUpdatesBefore.clear();

        DescriptiveStatistics statsLargeIncrementalListUpdateBefore = new DescriptiveStatistics();
        DescriptiveStatistics statsLargeIncrementalListUpdateAfter = new DescriptiveStatistics();

        for (String key : beforeDataObject.getJSONObject("itemsToResponseTime").keySet()) {
            statsLargeIncrementalListUpdateBefore.addValue(beforeDataObject.getJSONObject("itemsToResponseTime").getDouble(key));
            statsLargeIncrementalListUpdateAfter.addValue(afterDataObject.getJSONObject("itemsToResponseTime").getDouble(key));
        }


        //infer from large incremental list updates test
        conductOneTailTest(statsLargeIncrementalListUpdateBefore.getSortedValues(), statsLargeIncrementalListUpdateAfter.getSortedValues(), 0.05, "To Reject Null_Hyp that large incremental list update performance remained unchanged after upgrade");

        Thread.sleep(1000 * 3);//just give perception of work so it doesn't load the next menu option so quickly
        System.out.println("Would you like to see the graph of the differences?");
        boolean launchVisual = parseBooleanInput(String.valueOf(new Scanner(System.in).hasNextLine()));

        if (launchVisual) {
            MetricsVisualization visualization = new MetricsVisualization();
            //start in new thread if its a new launch
            if (appRunCounter == 0) {
                //wrap the app in a new Thread
                Thread viz = new Thread() {
                    public void run() {
                        visualization.beforeResponseTime_SingleUpdate = beforeDataObject.getJSONObject("last_Hundred_Single_Updates").toMap();
                        visualization.afterResponseTime_SingleUpdate = afterDataObject.getJSONObject("last_Hundred_Single_Updates").toMap();
                        visualization.beforeResponseTime_SmallIncUpdate = beforeDataObject.getJSONObject("last_Hundred_NPlusOneLists_Updates").toMap();
                        visualization.afterResponseTime_SmallIncUpdate = afterDataObject.getJSONObject("last_Hundred_NPlusOneLists_Updates").toMap();
                        visualization.chartTitle = "Voldemort Update Response Time";
                        visualization.parentRunCount = appRunCounter + 1;
                        visualization.launchChart();
                    }
                };
                viz.start();
            } else {
                String charTitle = beforeResponseTimeFile.substring(0, beforeResponseTimeFile.length() - 5).toUpperCase() + " vs " + afterResponseTimeFile.substring(0, afterResponseTimeFile.length() - 5).toUpperCase();
                visualization.parentRunCount = appRunCounter;
                visualization.chartUpdateCount = appRunCounter - 1;
                visualization.beforeResponseTime_SingleUpdate = beforeDataObject.getJSONObject("last_Hundred_Single_Updates").toMap();
                visualization.afterResponseTime_SingleUpdate = afterDataObject.getJSONObject("last_Hundred_Single_Updates").toMap();
                visualization.beforeResponseTime_SmallIncUpdate = beforeDataObject.getJSONObject("last_Hundred_NPlusOneLists_Updates").toMap();
                visualization.afterResponseTime_SmallIncUpdate = afterDataObject.getJSONObject("last_Hundred_NPlusOneLists_Updates").toMap();
                visualization.chartTitle = "Voldemort Update Response Time";
                visualization.update(new Observable(), charTitle); // really unnecessary to use to implement observer when everything is public static!
            }
        }
    }

    /*
    A pairwise sample test is different from a regular t-test in that it compares the same list and user, where as a regular t-test can compare
    two different x-independent variable (username) under the same condition
    http://blog.minitab.com/blog/statistics-and-quality-data-analysis/t-for-2-should-i-use-a-paired-t-or-a-2-sample-t
     */
    private void conductPairwiseTTest(double[] before, double[] after) throws Exception {
        Thread.sleep(1000);
        System.out.println("Conducting Pairwise TTest on last_Hundred_Single_Updates..");
        boolean reject_null = TestUtils.pairedTTest(before, after, 0.5);
        if (reject_null) {
            System.out.println("NULL_HYPOTHESIS_REJECTED under Pairwise TTest - Alpha Level 0.05");
        } else {
            System.out.println("FAILED_TO_REJECT_NULL_HYP under Pairwise TTest - Alphal Level 0.05");
        }
    }

    /*
    @todo printout useful statistics info about the experiments data
    @sampleFile - file that contains two sample data
     */
    public void getDescriptiveStatistics(String sampleFile) throws Exception {
        String data = convertFileContentToData(System.getProperty("user.dir") + "/tmp/" + sampleFile);
        JSONObject samples = convertToJSONObject(data);

        DescriptiveStatistics stats1 = new DescriptiveStatistics();
        DescriptiveStatistics stats2 = new DescriptiveStatistics();
        JSONArray sample1 = samples.getJSONArray("sample1");
        JSONArray sample2 = samples.getJSONArray("sample2");
        double trimmedMean1, trimmedMean2;
        double SE_Mean1, SE_Mean2, Mean_s1, Mean_s2, Mean_TR1, Mean_TR2, StDev1, StDev2, Min1, Min2, Q1_1, Q1_2, Q3_1, Q3_2;

        for (Object o : sample1.toList()) {
            stats1.addValue((double) o);
        }
        trimmedMean1 = getTrimmedMean_TR(stats1.getSortedValues(), 5);
        SE_Mean1 = getStandardError_SE(stats1.getStandardDeviation(), stats1.getN());

        for (Object o : sample2.toList()) {
            stats2.addValue((double) o);
        }
        trimmedMean2 = getTrimmedMean_TR(stats2.getSortedValues(), 5);
        SE_Mean2 = getStandardError_SE(stats2.getStandardDeviation(), stats2.getN());

        System.out.printf("%50s %22s %n", "-------Descriptive Summary Statistics (", "Sample 1" + " )-------");
        System.out.printf("%-15s %15s %15s %15s %15s   %n", "Variable N", "Mean", "TR Mean", "StDev", "SE Mean");
        System.out.printf("%-15s %15f %15f %15f %15f   %n", String.valueOf(stats1.getN()), stats1.getMean(), trimmedMean1, stats1.getStandardDeviation(), SE_Mean1);
        System.out.printf("%-15s %15s %15s %15s   %n", "Min", "Max", "Q1", "Q3");
        System.out.printf("%-15f %15s %15s %15s  %n%n", stats1.getMin(), stats1.getMax(), stats1.getPercentile(25.0), stats1.getPercentile(75.0));

        System.out.printf("%50s %22s %n", "-------Descriptive Summary Statistics (", "Sample 2" + " )-------");
        System.out.printf("%-15s %15s %15s %15s %15s   %n", "Variable N", "Mean", "TR Mean", "StDev", "SE Mean");
        System.out.printf("%-15s %15f %15f %15f %15f   %n", String.valueOf(stats2.getN()), stats2.getMean(), trimmedMean2, stats2.getStandardDeviation(), SE_Mean2);
        System.out.printf("%-15s %15s %15s %15s   %n", "Min", "Max", "Q1", "Q3");
        System.out.printf("%-15f %15s %15s %15s  %n", stats2.getMin(), stats2.getMax(), stats2.getPercentile(25.0), stats2.getPercentile(75.0));
    }

    //get descriptive stats summary given an array of sample values
    public void getDescriptiveStatistics(double[] sample, String metaDescription) throws Exception {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        double TrimmedMean_TR, StandardErrorMean_SE, Mean, StandardDeviation_STDev, Min, Max, Q1, Q3;

        for (double d : sample) {
            stats.addValue(d);
        }
        StandardDeviation_STDev = stats.getStandardDeviation();
        TrimmedMean_TR = getTrimmedMean_TR(stats.getSortedValues(), 5);
        StandardErrorMean_SE = getStandardError_SE(StandardDeviation_STDev, stats.getN());
        Min = stats.getMin();
        Max = stats.getMax();
        Mean = stats.getMean();
        Q1 = stats.getPercentile(25.0);
        Q3 = stats.getPercentile(75.0);
        System.out.printf("%50s %22s %n", "-------Descriptive Summary Statistics (", metaDescription + " )-------");
        System.out.printf("%-15s %15s %15s %15s %15s   %n", "Variable N", "Mean", "TR Mean", "StDev", "SE Mean");
        System.out.printf("%-15s %15f %15f %15f %15f   %n", String.valueOf(sample.length), Mean, TrimmedMean_TR, StandardDeviation_STDev, StandardErrorMean_SE);
        System.out.printf("%-15s %15s %15s %15s   %n", "Min", "Max", "Q1", "Q3");
        System.out.printf("%-15f %15s %15s %15s  %n", Min, Max, Q1, Q3);
    }

    /*
    Get the t-critical value given a degree of freedom and an alpha level
     */
    public double getTCritical(int degOfFreedom, double alphaLevel) throws Exception {
        TDistribution tDistribution = new TDistribution(degOfFreedom);
        return tDistribution.inverseCumulativeProbability(1 - (alphaLevel) / 2);
    }

    //Apache Math common doesn't give a function to get standard error - this is the formula
    public double getStandardError_SE(double standardDev, long sampleSizeN) throws Exception {
        return standardDev / Math.sqrt(sampleSizeN);
    }

    //most stats seems to say TR value is calculated for 5% of top & bottom data removed
    //http://www.oswego.edu/~srp/stats/tr_mean.htm
    public double getTrimmedMean_TR(double[] sortedSampleValues, double trimPercent) {
        double percentTrim = (trimPercent / 100);
        int startOffset = (int) Math.round(sortedSampleValues.length * percentTrim);

        DescriptiveStatistics statistics = new DescriptiveStatistics();
        for (int i = startOffset; i < sortedSampleValues.length - startOffset; i++) {
            statistics.addValue(sortedSampleValues[i]);
        }
        return statistics.getMean();
    }

    public double poolStandardErrors(double sample1_stdev, double sample2_stdev, long sampleSize1, long sampleSize2) {
        double sep = ((sample1_stdev + sample2_stdev) / 2) * Math.sqrt((1.0 / sampleSize1) + (1.0 / sampleSize2));
        return sep;
    }

    /* calculate the t-statistics of the two samples */
    public double getT_Statistics(double sample_mean1, double sample_mean2, double poolStandardErrors) {
        return (sample_mean1 - sample_mean2) / poolStandardErrors;
    }

    //assumes a two-sample t-test, one tailed
    public int getDegreesOfFreedom(double sampleSize1, double sampleSize2) {
        return (int) (sampleSize1 + sampleSize2) - 2;
    }

    /*
    2 Sample t-Test (1 tailed, assumes equal variance) - based on the paper
    https://www.unm.edu/~marcusj/2Sampletex1.pdf
     */
    public String conductOneTailTest(double[] sample1, double[] sample2, double alphaLevel, String testDescription) throws Exception {
        //Null_Hyp: sample_mean2 - sample_mean1 = 0 there is no difference in (speed/performance/etc)
        //Alt_Hyp: sample_mean2 - sample_mean1 <= 0 original responsetime/resource usage is more than new change

        getDescriptiveStatistics(sample1, "Sample 1: Before Upgrade"); //summarize the descriptive stats for sample 1
        getDescriptiveStatistics(sample2, "Sample 2: After Upgrade"); //summarize the descriptive stats for sample 1

        DescriptiveStatistics stats1 = new DescriptiveStatistics();
        DescriptiveStatistics stats2 = new DescriptiveStatistics();

        System.out.println("\nRunning statistical tests: " + testDescription);
        for (double d1 : sample1) {
            stats1.addValue(d1);
        }
        for (double d2 : sample2) {
            stats2.addValue(d2);
        }

        double sample1_stdev = stats1.getStandardDeviation();
        double sample1_stdErr = getStandardError_SE(sample1_stdev, stats1.getN());
        double sample1_mean = stats1.getMean();
        double sample2_stdev = stats2.getStandardDeviation();
        double sample2_stdErr = getStandardError_SE(sample2_stdev, stats2.getN());
        double sample2_mean = stats2.getMean();

        double poolStandardError_SE = poolStandardErrors(sample1_stdev, sample2_stdev, stats1.getN(), stats2.getN());
        double tSTAT = getT_Statistics(sample1_mean, sample2_mean, poolStandardError_SE);
        int degFreedom = getDegreesOfFreedom(stats1.getN(), stats2.getN());

        double tCRIT = getTCritical(degFreedom, alphaLevel);
        // failed reject the NULL_HYP that the mean difference b/w the two are statistically zero
        if (Math.abs(tSTAT) < tCRIT) {
            System.out.printf("\nACCEPT NULL HYPOTHESIS: tStat(|%f|) < tCRIT(%f) : %b %n", tSTAT, tCRIT, new Boolean(Math.abs(tSTAT) < tCRIT));
            rejectNULLHypothesisByConfidenceLimit(sample1_mean, sample2_mean, tCRIT, poolStandardError_SE);
            return "FAILED_TO_REJECT_NULL_HYP";
        } else {
            System.out.printf("\nACCEPT NULL HYPOTHESIS: tStat(|%f|) < tCRIT(%f) : %b %n", tSTAT, tCRIT, new Boolean(Math.abs(tSTAT) < tCRIT));
            rejectNULLHypothesisByConfidenceLimit(sample1_mean, sample2_mean, tCRIT, poolStandardError_SE);
            return "NULL_HYP_REJECTED";
        }
    }

    /* read from file so it can ge more applicable to different tests */
    public String conductOneTailTest(String sampleFileName, double alphaLevel, String testDescription) throws Exception {
        String data = convertFileContentToData(System.getProperty("user.dir") + "/tmp/" + sampleFileName);
        JSONObject samples = convertToJSONObject(data);

        DescriptiveStatistics stats1 = new DescriptiveStatistics();
        DescriptiveStatistics stats2 = new DescriptiveStatistics();
        JSONArray sample1 = samples.getJSONArray("sample1");
        JSONArray sample2 = samples.getJSONArray("sample2");

        for (Object o : sample1.toList()) {
            stats1.addValue((double) o);
        }

        for (Object o : sample2.toList()) {
            stats2.addValue((double) o);
        }
        return conductOneTailTest(stats1.getSortedValues(), stats2.getSortedValues(), alphaLevel, "Check that sample2 is different or better than sample1 with alphaLevel=0.05");
    }

    /*
    Implement confidence interval test -- another way to show whether we can accept the NULL_HYPOTHESIS that that there are staticically no difference between the sample means
    This is valid for a two-sampled one tail test according to this paper https://www.unm.edu/~marcusj/2Sampletex1.pdf
     */
    public String rejectNULLHypothesisByConfidenceLimit(double sample1_mean, double sample2_mean, double tCRIT, double standErrorPool) throws Exception {
        //LL = (Y1_bar - Y2_bar) - tCRIT*sep
        //LU = (Y1_bar - Y2_bar) + tCRIT*sep
        double LowerLimit = (sample1_mean - sample2_mean) - (tCRIT * standErrorPool);
        double UpperLimit = (sample1_mean - sample2_mean) + (tCRIT * standErrorPool);

        System.out.println("Verifying Conclusion With Confidence Limit Tests...");
        System.out.println("Lower Limit Encompasses Zero: " + (LowerLimit <= 0));
        System.out.println("Upper Limit Encompasses Zero: " + (UpperLimit >= 0));

        boolean accept_null_hyp = (LowerLimit <= 0) && (UpperLimit >= 0); //NULL_HYP zero falls within the limit or not?

        if (accept_null_hyp) {
            System.out.println("FAILED_TO_REJECT Null Hypothesis - LL=" + LowerLimit + "<= 0 <=" + UpperLimit + " LU\n");
            return "FAILED_TO_REJECT";
        } else {
            System.out.println("NULL_HYP_REJECTED - LL=" + LowerLimit + "<= 0 <=" + UpperLimit + " LU Does Not Hold\n");
            return "NULL_HYP_REJECTED";
        }
    }

    /*
     get some real test data from some real core to test asset-mapping
     we need to get episodes that have linearOffers -- post on these episodes will run through the whole asset-mapper
     -- map eps to series, map station to channel (station callSign), station to network (stationGroup)
     -- we can use this to confirm that we get groups info to pass through to si consumption cube for example
     */
    public String getRealLinearOffersTestData(String searchTapURL, int totalAssets, int startOffset) throws Exception {
        JSONObject testData = new JSONObject();
        String data = "";
        int offset = startOffset;
        JSONArray response = sendGetEventToEndPoint(epsDataURL + "?q=*:*&__fq=linearOffers:*&__fq=*:*%20-%20objectType:Series&__fl=episodeTitle,objectType,title,id,linearOffers.stationId,linearOffers.duration,groupId,seriesId&__limit=10&__enable.merchandising=false&__offset=" + offset, true);
        int totalHitCount = Integer.parseInt(response.getJSONObject(0).get("hitCount").toString());
        if (totalHitCount < totalAssets) {
            System.out.println("There is not enough linear offers in this core to scrape");
            return null;
        }

        JSONArray hitsArray = response.getJSONObject(0).getJSONArray("hits");
        // go through the hits array and scrape for episodes and their offers stationId
        for (int i = 0; i < hitsArray.length(); i++) {
            JSONObject asset = hitsArray.getJSONObject(i);
            String objectType = asset.getString("objectType");
            testData.put("id", asset.get("id"));
            testData.put("title", asset.get("title"));
            testData.put("objectType", objectType);
            testData.put("stationId", asset.getJSONArray("linearOffers").getJSONObject(0).get("stationId"));
            testData.put("duration", asset.getJSONArray("linearOffers").getJSONObject(0).get("duration"));
            testData.put("stationId", asset.getJSONArray("linearOffers").getJSONObject(0).get("stationId"));
            if (objectType.equalsIgnoreCase("Episode")) {
                testData.put("seriesId", asset.get("seriesId"));
                try {
                    testData.put("title", asset.get("episodeTitle"));
                } catch (JSONException e) {
                    testData.put("title", asset.get("title"));
                }
            } else if (objectType.equalsIgnoreCase("SportsEvent")) {
                testData.put("seriesId", asset.getJSONArray("linearOffers").getJSONObject(0).get("seriesId")); // SportsEvent doesn't have seriesId at top-level asset, only in its linearOffers
            }
            data += testData + ",";
        }
        startOffset += 10; // scrapes by interval of 10 assets
        if (startOffset == (totalAssets-10)) { // i.e if 1000, we stop at 990, if 10, we stop at 0
            return data;
        } else return data + "" + getRealLinearOffersTestData(searchTapURL, totalAssets, startOffset);
    }

    /* scrap the core for some real episodes, movies, programs and sports events that have linear offers which we can use for some testing */
    public void createRealLinearOffersTestData(String searchTapURL, int totalAssets, int startOffset, String fileName) throws Exception {
        File file = new File(System.getProperty("user.dir") + "/tmp/" + fileName);
        if (file.exists()) {
            System.out.printf("\nTest Data %s already exists\n", fileName);
            return;
        }

        String data = getRealLinearOffersTestData(searchTapURL, totalAssets, startOffset);
        data = "[" + data.substring(0, data.length() - 1) + "]";

        JSONArray dArray = new JSONArray(data);
        JSONObject dataSummary = new JSONObject();
        dataSummary.put("data_summary", new JSONObject());
        String title = "";
        String objectType = "";
        //add additional summary to the test data, might come in useful
        for (int i = 0; i < dArray.length(); i++) {
            JSONObject asset = dArray.getJSONObject(i);

            objectType = asset.getString("objectType");
            title = asset.getJSONObject("title").toMap().values().toString();
            title = title.substring(1,title.length()-1);

            //de-duplicate stationIds
            if(i==0){
                //make sure at least one station is added in first before checking
                dataSummary.getJSONObject("data_summary").append("stationIds", asset.getString("stationId"));
            } else if(!dataSummary.getJSONObject("data_summary").getJSONArray("stationIds").toList().contains(asset.getString("stationId"))){
                dataSummary.getJSONObject("data_summary").append("stationIds", asset.getString("stationId"));
            }
            dataSummary.getJSONObject("data_summary").append("titles", title);
            if (objectType.equalsIgnoreCase("Episode")) {
                dataSummary.getJSONObject("data_summary").append("episodeIds", asset.getString("id"));
            } else if (objectType.equalsIgnoreCase("sportsEvent")) {
                dataSummary.getJSONObject("data_summary").append("sportsEventIds", asset.getString("id"));
            } else if (objectType.equalsIgnoreCase("Program")) {
                dataSummary.getJSONObject("data_summary").append("programIds", asset.getString("id"));
            }
        }

        dArray.put(dataSummary);
        File output = new File(System.getProperty("user.dir") + "/tmp/" + fileName);
        FileWriter writer2 = new FileWriter(output);
        writer2.write(String.valueOf(dArray));
        writer2.close();
        Thread.sleep(3000); //wait for write to catch up?
        File fileCheck = new File(System.getProperty("user.dir") + "/tmp/" + fileName);
        if (fileCheck.exists()) {
            System.out.printf("\nTest Data successfully created see %s\n", fileName);
            buildStationToChannelNetworkMapping(searchTapURL,dataSummary.getJSONObject("data_summary").getJSONArray("stationIds").toList(),fileName);
            return;
        }
    }

    private String epsDataURL = "https://internal-app-network.net/sd/tivo-nlu/taps/standard/search";

    //just more convenient method to see which networks and channels the stations we post in were mapped to
    // of course we can check this manually in ffs when we are curious about those individual mapping -- but having this
    //map be of value to future testing strategy
    public void buildStationToChannelNetworkMapping(String coreURL,List <Object> stationIds,String fileName) throws Exception{
        String filter="fl=callSign,stationGroup&enable.merchandising=false";
        String basePopURL = coreURL.substring(0,coreURL.indexOf("taps"));
        String data = sendGetEventToEndPoint(basePopURL);
        JSONObject popDescriptor = new JSONObject(data);
        String assetsCore ="";
        String channel="FXM";
        String networkGroup="FXMOVIE";
        JSONObject stationToChannelNetworkMapping = new JSONObject();
        stationToChannelNetworkMapping.put("st-4333273045",new JSONObject().put("channel",channel).put("networkGroup",networkGroup));
        try{
            assetsCore = popDescriptor.getString("assetsCore");
        } catch (Exception e){
            assetsCore = "catalog-"+popDescriptor.getString("catalogId")+"-assets";
        }
        String stationCoreSearchURL = basePopURL.substring(0,basePopURL.indexOf("net")+4)+"ffs/"+assetsCore+"/search?"+filter+"&q=id:";

        for(Object station: stationIds){
            try {
                JSONObject hitsResponse = new JSONObject(sendGetEventToEndPoint(stationCoreSearchURL+station));
                channel = hitsResponse.getJSONArray("hits").getJSONObject(0).getString("callSign");
                networkGroup = hitsResponse.getJSONArray("hits").getJSONObject(0).getString("stationGroup");


                stationToChannelNetworkMapping.put(station.toString(),new JSONObject().put("channel",channel).put("networkGroup",networkGroup));
            } catch (Exception e){
                //no hits found
            }
        }
        String outFileName = System.getProperty("user.dir") + "/tmp/channelNetworkMapping-" + fileName;
        File output = new File(outFileName);
        FileWriter writer2 = new FileWriter(output);
        writer2.write(String.valueOf(stationToChannelNetworkMapping));
        writer2.close();
        Thread.sleep(3000); //wait for write to catch up?
        File fileCheck = new File(outFileName);
        if (fileCheck.exists()) {
            System.out.printf("\nTest channelNetwork Mapping test data successfully created see %s\n", outFileName);
            return;
        }
    }

    //generate random queries for tivo-nlu, genericize this later, but this method is specifically make to test don-si's tivo-nlu pop on search tap
    public void generateRandomQueriesForTivoNLU(int totalQueries, int totalUsers) throws Exception{
        String randomTitle = "";
        String tivo_nlu_searchTap="https://internal-app-network.net/sd/tivo-nlu/taps/standard/search?__enable.merchandising=false&__qf=id%20title%20episodeTitle";
        String userID = generateLongUID();
        int userCounter=0;
        int viewCount = 0;
        HashMap <String, Integer> networkCounts = new HashMap<>(); // let's see how many networks were mapped as part of this random test data
        HashMap <String, Integer> channelCounts = new HashMap<>(); // let's see how many networks were mapped as part of this random test data
        //total number of queries
        long NOW = System.currentTimeMillis()-TimeUnit.HOURS.toMillis(1); //set now of query to last hour
        for(int i=0; i<totalQueries; i++){
            userCounter++;
            HashMap randomAsset =  getRandomTitleFromTivoCore();
            randomTitle =hexEncodedTitle(randomAsset.get("title").toString());
            String queryResponse = sendGetEventToEndPoint(tivo_nlu_searchTap+"&q="+randomTitle+"&userId="+userID+userCounter+"__NOW="+NOW);

            //send queryid for only 50% of the views -- leaving half to be "soft-conversion"
            if(i%2==0){
                randomAsset.put("queryId",new JSONObject(queryResponse).getString("queryId"));
            }
            long startTime = System.currentTimeMillis()-TimeUnit.SECONDS.toMillis(Long.parseLong(randomAsset.get("duration").toString()));
            long endTime = System.currentTimeMillis();
            String data = generateViewEventData(userID+userCounter,String.valueOf(randomAsset.get("id")),startTime,endTime,randomAsset);
            postEventToEndpoint("["+data+"]","https://internal-app-network.net/sd/tivo-nlu/viewEvents",false);

            String network = this.TIVO_CHANNEL_NETWORK.getJSONObject(randomAsset.get("stationId").toString()).getString("networkGroup");
            String channel = this.TIVO_CHANNEL_NETWORK.getJSONObject(randomAsset.get("stationId").toString()).getString("channel");
            if(networkCounts.get(network)==null){
                networkCounts.put(network,1);
            } else {
                networkCounts.put(network,networkCounts.get(network)+1);
            }
            if(channelCounts.get(channel)==null){
                channelCounts.put(channel,1);
            } else {
                channelCounts.put(channel,channelCounts.get(channel)+1);
            }
            if(userCounter==totalUsers) userCounter=0; //reset user counter -- generate queries as the same from before
        }

        long testDuration = (System.currentTimeMillis()-NOW)*(1l/(1000*60));//convert to minutes
        String summary = "Test ran for "+testDuration+" minutes with "+totalQueries+" total queries and "+totalUsers+" total users\n";
        String outFileName = System.getProperty("user.dir") + "/tmp/tivo_nlu_query-test-"+NOW+".txt";
        File output = new File(outFileName);
        FileWriter writer2 = new FileWriter(output);
        writer2.write(summary);
        writer2.write(String.valueOf(channelCounts));
        writer2.write(String.valueOf(networkCounts));
        writer2.close();
    }

    public String hexEncodedTitle(String title) {
        return title.replaceAll(" ","%20");
    }

    public HashMap getRandomTitleFromTivoCore() throws Exception{
        int randSeed = new Random().nextInt(TIVO_ASSETS_W_OFFERS.length()-2); //length-1 == summary_data object, length-2 contains all the title and station
        String title = TIVO_ASSETS_W_OFFERS.getJSONObject(randSeed).getJSONObject("title").toMap().values().toString();
        int duration =  TIVO_ASSETS_W_OFFERS.getJSONObject(randSeed).getInt("duration");
        String id =   TIVO_ASSETS_W_OFFERS.getJSONObject(randSeed).getString("id");
        String stationId =   TIVO_ASSETS_W_OFFERS.getJSONObject(randSeed).getString("stationId");
        title = title.substring(1,title.length()-1);
        HashMap map = new HashMap();
        map.put("title",title);
        map.put("duration",duration);
        map.put("stationId",stationId);
        map.put("id",id);
        return map;
    }

    /*
    Look up stationId from the tivo-nlu core that is built on startup
    Use this method to determine whether we have a valid station for a given item we got back from search results so we can send it along for our hard conversion
     */
    public String getStationId(String itemId) throws Exception{
        String stationId = null;
        int randSeed = new Random().nextInt(TIVO_ASSETS_W_OFFERS.length()-2); //length-1 == summary_data object, length-2 contains all the title and station

        //iteration through our offers test data to find an object with the given itemId to pluck a station for
        for(int i=0; i<TIVO_ASSETS_W_OFFERS.length()-2; i++){
            JSONObject itemObject = TIVO_ASSETS_W_OFFERS.getJSONObject(i);
            if(itemId.equalsIgnoreCase(itemObject.getString("id"))){
                stationId = itemObject.getString("stationId");
                break;
            }
        }
        return stationId;
    }



    /*
    Generate some queries and events to feed the Use Case Performance Overview Dashboard in the Product Module
    @Param useCaseConfigFileName - the configuration file to base our queries and event data properties on
    This needs to be genericized, somewhat limited to to tivo-nlu tap definitions
    Background: SI will only calculate conversion for first 10 results - does not matter if it is displayed or not
    Background: In soft conversion if two taps had offer same results, soft conversion will be credited to both taps
     */
    public void generateUseCasePerformanceData(String useCaseConfigFileName) throws Exception{
        JSONObject config = new JSONObject(convertFileContentToData(System.getProperty("user.dir")+"/tmp/"+useCaseConfigFileName));


        JSONObject conversionHistory = new JSONObject();
        conversionHistory.put("titles_seen_by_prefix_byUser", new JSONObject());

        QAHelper helper = this;
        long threadStartTime = System.currentTimeMillis();
        Thread pfThread = new Thread(){
            public void run() {
                try {
                    new EventQueryWork(EventQueryWork.UseCase.PREFIX_SEARCH,helper,config);
                } catch (Exception e){
                    System.out.println("PREFIX_WORK_ERROR "+e.getMessage());
                }
            }
        }; pfThread.start();

        Thread ftThread = new Thread(){
            public void run() {
                try {
                     new EventQueryWork(EventQueryWork.UseCase.FULLTEXT_SEARCH,helper,config);
                } catch (Exception e){
                    System.out.println("FT_WORK_ERROR "+e.getMessage());
                }
            }
        }; ftThread.start();

        Thread mltThread = new Thread(){
            public void run() {
                try {
                    new EventQueryWork(EventQueryWork.UseCase.MLT_SEARCH,helper,config);
                } catch (Exception e){
                    System.out.println("MLT_WORK_ERROR "+e.getMessage());
                }
            }
        }; mltThread.start();


        Thread predictionsThread = new Thread(){
            public void run() {
                try {
                    new EventQueryWork(EventQueryWork.UseCase.PREDICTIONS,helper,config);
                } catch (Exception e){
                    System.out.println("PREDICTIONS_WORK_ERROR "+e.getMessage());
                }
            }
        }; predictionsThread.start();


        predictionsThread.join();
        mltThread.join();
        ftThread.join();
        pfThread.join();

        long threadEndTime = System.currentTimeMillis();
        long duration = threadEndTime - threadStartTime;

        String summary = String.format("\nTest generated %d query events and conversions in %d hour(s) %d minute(s) and %d seconds(s)\n", config.getInt("totalEvents"),TimeUnit.MILLISECONDS.toHours(duration),TimeUnit.MILLISECONDS.toMinutes(duration),TimeUnit.MILLISECONDS.toSeconds(duration));
        summary = summary.concat("\nPrefix Conversion "+prefix_hard_conversions);
        summary = summary.concat("\nFT Conversion "+ft_hard_conversions);
        summary = summary.concat("\nMLT Conversion "+mlt_hard_conversions);
        summary = summary.concat("\nPredictions Conversion "+predictions_hard_conversions);
        summary = summary.concat("\n"+use_case_conversion_summary);
        summary = summary.concat("\nPrefix Search #Unique User Hard Conversion "+prefix_hard_conversions.size());
        summary = summary.concat("\nFullText Search #Unique User Hard Conversion "+ft_hard_conversions.size());
        summary = summary.concat("\nMLT Search #Unique User Hard Conversion "+mlt_hard_conversions.size());
        summary = summary.concat("\nPredictions #Unique User Hard Conversion "+predictions_hard_conversions.size());
        summary = summary.concat("\n"+"Test started at: "+getLocalDateTime(threadStartTime)+" and Ended at: "+getLocalDateTime(threadEndTime));
        System.out.println(summary);
        String summaryFileName = System.getProperty("user.dir")+"/tmp/"+"EventGenerator_"+getLocalDateTime(System.currentTimeMillis())+".txt";
        File output = new File(summaryFileName);
        FileWriter writer = new FileWriter(output);
        writer.write(summary);
        writer.close();
        Thread.sleep(3000); //wait for write to catch up
        File fileCheck = new File(summaryFileName);
        if (fileCheck.exists()) {
            System.out.printf("\nEvent generation summary can be found at %s\n", summaryFileName);
        }

        EventQueryWork queryWork = new EventQueryWork(this,config);
        queryWork.generateExpectedConversionReportFromTest(threadStartTime,threadEndTime);
    }


    public void runExperimentsTest(String experimentConfig) throws Exception{
        JSONObject config = new JSONObject(convertFileContentToData(System.getProperty("user.dir")+"/tmp/"+experimentConfig));

        QAHelper helper = this;
        long startTime = System.currentTimeMillis();

                try {
                    EventQueryWork mltExperiment = new EventQueryWork(EventQueryWork.UseCase.EXPERIMENT,helper,config);
                    long endTime = System.currentTimeMillis();
                    long duration = endTime - startTime;

                    String summary = String.format("\nTest generated %d query events and conversions in %d hour(s) %d minute(s) and %d seconds(s)\n", config.getInt("totalEvents"),TimeUnit.MILLISECONDS.toHours(duration),TimeUnit.MILLISECONDS.toMinutes(duration),TimeUnit.MILLISECONDS.toSeconds(duration));
                    summary = summary.concat("\n"+"Test started at: "+getLocalDateTime(startTime)+" and Ended at: "+getLocalDateTime(endTime));
                    summary = summary.concat("\n"+"Unique experiment conversion by all users: "+mltExperiment.getUniqueConversionsByAllUsers());
                    summary = summary.concat("\n"+prettyPrintMap(experiment_conversions));
                    System.out.println(summary);
                    String summaryFileName = System.getProperty("user.dir")+"/tmp/"+"ExperimentRun_"+getLocalDateTime(System.currentTimeMillis())+".txt";
                    File output = new File(summaryFileName);
                    FileWriter writer = new FileWriter(output);
                    writer.write(summary);
                    writer.close();
                    Thread.sleep(3000); //wait for write to catch up
                    File fileCheck = new File(summaryFileName);
                    if (fileCheck.exists()) {
                        System.out.printf("\nExperiment summary can be found at %s\n", summaryFileName);
                    }

                    mltExperiment.generateExpectedConversionReportFromTest(startTime,endTime,true);

                } catch (Exception e){
                    System.out.println("Experiment Error "+e.getMessage());
                }

    }


    /* just care about tracking hard conversion as building map for hundreds of thousands of users will cost our limited memory -- future implementation we can store them in a database instead*/
    public static volatile HashMap <String, Set> prefix_hard_conversions = new HashMap<>();
    public static volatile HashMap <String, Set> mlt_hard_conversions = new HashMap<>();
    public static volatile HashMap <String, Set> predictions_hard_conversions = new HashMap<>();
    public static volatile HashMap <String, Set> ft_hard_conversions = new HashMap<>();
    public static volatile Map <String, Integer> experiment_conversions = new HashMap<>();
    public static volatile StringBuilder use_case_conversion_summary = new StringBuilder();

    // get a random title and break it down into smaller pieces to give to prefix search so we get more results rather than exact result
    public String getRandomPrefixSearchTerms() throws Exception{
        String prefixTitle = getRandomTitleFromTivoCore().get("title").toString();
        String [] words_in_title = prefixTitle.split(" ");
        String title = "star trek"; //default
        if(words_in_title.length>1){
            boolean multi_wordpref = new Random().nextBoolean();
            //get at least 2 words
            if(multi_wordpref){
                int startIndex = 0;
                try{
                    startIndex = new Random().nextInt(words_in_title.length-2);
                } catch (Exception e){ //use default zero starting offset
                }
                title = words_in_title[startIndex]+"%20"+words_in_title[startIndex+1]; //also encode any spacing
            } else {
                title = words_in_title[new Random().nextInt(words_in_title.length)];
            }
        } else title = words_in_title[0];
        return title;
    }

    public String[] collectHitIds(JSONArray hits,String resultType) throws Exception{
        String [] ids = new String[hits.length()*25];
        int totalList = 0;
        int totalGroups = 0;
        for (int i=0; i<hits.length(); i++){
            // if it is a list -- get results from the items array which is found in the list array
            if(resultType.equalsIgnoreCase("lists")){
                JSONArray items = hits.getJSONObject(i).getJSONArray("items"); //get the items array length
                int itemSize = items.length();
                for(int j=0; j<itemSize; j++){  //iterate through the items array and collect the id
                    ids[j+totalList]=items.getJSONObject(j).getString("id");
                    totalList++;
                }
            } else if(resultType.equalsIgnoreCase("hits")){
                ids[i] = hits.getJSONObject(i).getString("id");
            } else if(resultType.equalsIgnoreCase("groups")){
                //iterate through each group and collect each groups' hits ids
                for(int k=0; k<hits.length();k++){
                    //for each group -- iterate through the hits to collect the ids
                    for(int l = 0; l<hits.getJSONObject(k).getJSONArray("hits").length(); l++){
                        if(l<hits.getJSONObject(k).getJSONArray("hits").length()){
                            ids[totalGroups+l]=hits.getJSONObject(k).getJSONArray("hits").getJSONObject(l).getString("id");
                            totalGroups++;
                        } else break;
                    }
                }
            }


        }
        return  ids;
    }

    public void appendArrayToJSONArray(JSONObject object,String key, String [] arrayValues) throws Exception{
        for(String value: arrayValues){
            object.append(key,value);
        }
    }

    /*
    make conversion maps more readable for text output to file
     */
    public String prettyPrintMap(Map map) throws Exception{
        String summary = "";
        Map<Object, Object> treeMap = new TreeMap<>(map); // tree maps automatically sorts the map by its key
        for(Object object: treeMap.entrySet()){
            summary+=object.toString()+"\n";
        }
        return  summary;
    }


    public void compareKafkaTopicMeta(String beforeTopicFileName, String afterTopicFileName, File kafka_report_file) throws Exception {
        KafkaTestUtil kafkaTestUtil = new KafkaTestUtil();
        kafkaTestUtil.compareKafkaTopics(beforeTopicFileName,afterTopicFileName,kafka_report_file);
    }


    private void initializeAuthenication(){
        try(InputStream input = QAHelper.class.getClassLoader().getResourceAsStream("auth.properties")){
            Properties prop = new Properties();

            prop.load(input);
            this.catAuth = prop.getProperty("catAuth");
            this.ffsAuth = prop.getProperty("ffsAuth");
            this.catAuth = Base64.getEncoder().encodeToString(catAuth.getBytes());
            this.ffsAuth = Base64.getEncoder().encodeToString(ffsAuth.getBytes());

        } catch (IOException ex){
            ex.printStackTrace();
        }
    }

    private static String catAuth;
    private static String ffsAuth;
}