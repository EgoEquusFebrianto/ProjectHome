package javaStandardClasses.readData;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class StreamReaderBasic {
    public static void main(String[] args) {
       try {
//           String streamUrl = "http://localhost:4000/stream/api/retail-transaction";
           String streamUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

           URL url = new URL(streamUrl);
           HttpURLConnection connection = (HttpURLConnection) url.openConnection();

           connection.setRequestMethod("GET");
           connection.setConnectTimeout(10_000);
           connection.setReadTimeout(30_000);
           connection.setRequestProperty("Accept", "text/event-stream");
           connection.setRequestProperty("User-Agent", "agent/1.0");
           connection.setRequestProperty("Cache-Control", "no-cache");

           int responseCode = connection.getResponseCode();

           if (responseCode == HttpURLConnection.HTTP_OK) {
               try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                   String line;
                   int eventCount = 0;

                   while ((line = reader.readLine()) != null) {
                       System.out.println(line);
                       eventCount++;
                   }
               }
           } else {
               System.out.println("Error " + responseCode);
           }

       } catch(Exception e) {
           e.printStackTrace();
       }

    }
}