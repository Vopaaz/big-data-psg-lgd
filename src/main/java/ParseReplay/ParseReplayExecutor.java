package ParseReplay;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.bson.Document;

public class ParseReplayExecutor {
    private final String version = "v0.1.1";

    public Document getReplayInfoDocument(String file, Long matchId) {
        try {
            Date start = new Date();
            Document document = new Document("matchid", matchId);
            document.append("combatlog", new Combatlog().getEvents(file));
            document.append("info", new Info().getInfo(file));
            document.append("chat", new Chat().getChat(file));
            document.append("lifestate", new Lifestate().getStates(file));
            Date end = new Date();

            addProvenance(document, start, end);
            return document;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    private void addProvenance(Document document, Date start, Date end) {
        ArrayList<HashMap<String, Object>> provenanceList = new ArrayList<HashMap<String, Object>>();
        HashMap<String, Object> provenance = new HashMap<String, Object>();
        provenance.put("name", "Parse .dem file to MongoDB");
        provenance.put("code", "ParseStore.ParseReplayExecutor.getReplayInfoDocument");
        provenance.put("version", version);
        provenance.put("start", start);
        provenance.put("end", end);
        provenance.put("elapsed_ms", end.getTime() - start.getTime());
        provenanceList.add(provenance);

        document.append("provenance", provenanceList);
    }

    public String getVersion() {
        return version;
    }

}
