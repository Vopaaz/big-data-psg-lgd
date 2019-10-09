package FetchStore;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.*;
import java.util.List;
import java.util.ArrayList;

public class OpendotaAPI {
    private final static String USER_AGENT = "Mozilla/5.0";
    private final static String AUTHORIZE_KEY = "";
    private final static String REPALYS = "https://api.opendota.com/api/replays";

    public static List<String> getRepInfo(List<Long> ids) throws Exception{
        if(ids.size() == 0) return new ArrayList<>();

        StringBuilder openAPIUrl = new StringBuilder();
        openAPIUrl.append(REPALYS);

        for(int i = 0; i < ids.size(); ++i) {
            if(i == 0) {
                openAPIUrl.append("?match_id=");
                openAPIUrl.append(ids.get(i));
            }
            else {
                openAPIUrl.append("&match_id=");
                openAPIUrl.append(ids.get(i));
            }
        }

        String url = openAPIUrl.toString();

        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("User-Agent", USER_AGENT);

        int responseCode = con.getResponseCode();
        System.out.println("Response code: " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        //print result
        String jsonString = response.toString();
        System.out.println(jsonString);
        JSONArray arr = new JSONArray(jsonString);

        List<String> list = new ArrayList<>();
        for(int i = 0; i < arr.length(); ++i) {
            JSONObject cur = arr.getJSONObject(i);
            long curMatchId = cur.getLong("match_id");
            long curCluster = cur.getInt("cluster");
            long curRepSalt = cur.getInt("replay_salt");
            String curRepURL = constructReplayURL( curCluster,curMatchId, curRepSalt);
            System.out.println(curRepURL);
            list.add(curRepURL);
        }
        return list;
    }

    private static String constructReplayURL(long cluster, long matchId, long replaySalt) {
        StringBuilder sb = new StringBuilder();

        // construct a link have following format
        // http://replay<cluster>.valve.net/570/<match_id>_<replay_salt>.dem.bz2
        sb.append("http://replay");
        sb.append(cluster);
        sb.append(".valve.net/570/");
        sb.append(matchId);
        sb.append("_");
        sb.append(replaySalt);
        sb.append(".dem.bz2");

        return sb.toString();
    }


}
