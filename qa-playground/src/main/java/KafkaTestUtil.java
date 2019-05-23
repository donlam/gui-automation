import org.json.JSONObject;

import org.junit.Assert;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/*
Helper class to do some simple validation of kafka producer/consumers
Parse through json file of kafka events and extract out their keys, and metadata information
 */
public class KafkaTestUtil {


    /*
    given two kafka topic retrieved using Jim's Kafka' consumer tool (one before some kafka producer/consumer upgrade and one after)
    verify that all they same key are put in the same partition
    Jim's tool may be run like this './kafka-consumer --topic view-events --autoOffsetReset earliest --startingOffset 175984'
    Output to a file any differences in partitioning of the same key
     */
    public static void compareKafkaTopics(String beforeTopicFile, String afterTopicFile, File outputFile) throws Exception{

        //store the topicKey of the first file and its metadata
        Map  <String, String> topicKeyMessage = new HashMap<>();
        String line = null;
        try (BufferedReader br = new BufferedReader(new FileReader(beforeTopicFile))) {
            while ((line = br.readLine()) != null) {
                JSONObject event = new JSONObject(line);
                topicKeyMessage.put(event.get("key").toString(),event.get("meta").toString());
            }
        } catch (Exception e) {
            throw new FileNotFoundException("Cannot find beforeTopicFile to read " + e.getMessage() + "\n\n");
        }
        String data = "";
        FileWriter fw = new FileWriter(outputFile,true);
        BufferedWriter bw = new BufferedWriter(fw);

        JSONObject event = new JSONObject(); //object to hold event from second file
        JSONObject originalMeta = new JSONObject(); //
        //we have stored in all the key and meta info from the first file now read through the second and compare the meta to confirm that every produced event gets sent
        //to the same partition if they belong to the same key i.e view-events are keyed on population and userId
        try (BufferedReader br = new BufferedReader(new FileReader(afterTopicFile))) {
            while ((line = br.readLine()) != null) {
                event = new JSONObject(line);
                try {
                    originalMeta = new JSONObject(topicKeyMessage.get(event.get("key").toString()));
                }catch (Exception a){
                    System.out.println(event.get("key").toString()+" key did not exist previously -- cannot compare partition assignment. Ignoring the new key due to: " + a.getMessage() + "\n\n");
                    data = event.get("key").toString()+"௵"+"NULL"+"௵"+event.get("meta").toString()+"௵"+"NEW_KEY\n";
                    bw.write(data);
                    continue;
                }
                int originalPartition = originalMeta.getInt("partition");
                int newPartition = -1;
                //compare the current key to existing key -- if they are the same, assert that they get put into to the same partition
                try{
                    if(topicKeyMessage.containsKey(event.get("key").toString())){
                        newPartition = event.getJSONObject("meta").getInt("partition");
                        Assert.assertTrue("original partition and new partition matched for key does not match ",originalPartition==newPartition);
                        System.out.printf("Looks good - key: %s\n",event.get("key").toString());
                    }
                }catch (Exception | AssertionError ae){
                    if(ae instanceof AssertionError){
                        System.out.printf("The new key is assigned to same partition: %s  original partition %d != %d new partition\n",topicKeyMessage.containsKey(event.get("key")),originalPartition,newPartition);
                        String newMeta = event.getJSONObject("meta").toString();
                        String originalData = topicKeyMessage.get(event.get("key").toString());
                        String key = event.get("key").toString();
                        System.out.println("we did find the key but no matching metada "+newMeta);
                        System.out.println("the original data "+originalData);
                        System.out.println("the key being compared "+key+"\n");
                        data = key+"௵"+originalData+"௵"+newMeta+"௵"+"Partition mismatched\n";
                        bw.write(data);
                    } else {
                        System.out.println("Failed to compared topic meta due to "+ae.getMessage());
                    }

                }
            }
        } catch (Exception e) {
            bw.close();
            fw.close();
            throw new Exception("Something went wrong while comparing.."+e.getMessage());
        }
        bw.close();
        fw.close();
        System.out.println("Check "+outputFile.getAbsolutePath()+" for comparison results (delim ௵)");
    }


}
