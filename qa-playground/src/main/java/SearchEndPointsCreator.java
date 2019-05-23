import java.io.*;

/*
Given a file with search endpoints in format of /sd/{popName}/taps/{tapName}?{params} return two
search endpoints delineated ௵ character so we can use as input to compare results against of searching against these two endpoint
 */
public class SearchEndPointsCreator {

    public void createSearchURLs(String searchEndPointFile,String url1, String url2,String delim) throws Exception{

       String line = null;
       String search = "";
       StringBuilder doc = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(searchEndPointFile))) {
            while ((line = br.readLine()) != null) {
                search = url1.concat(line).concat(delim).concat(url2).concat(line)+"\n";
                doc.append(search);
            }
        } catch (Exception e) {
            throw new FileNotFoundException("Cannot find beforeTopicFile to read " + e.getMessage() + "\n\n");
        }

        File output = new File("searchEndpoints3.txt");
        FileWriter writer2 = new FileWriter(output);
        writer2.write(String.valueOf(doc));
        writer2.close();
    }

    public static void main(String args[]){
        SearchEndPointsCreator dr = new SearchEndPointsCreator();
        try {
            dr.createSearchURLs("/home/data/Work/qa-playground/endpoints3","http://ffs-localhost1.net","ffs-localhosthost2.net","௵");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
