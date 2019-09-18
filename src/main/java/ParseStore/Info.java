package ParseStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Descriptors.FieldDescriptor;

import skadistats.clarity.Clarity;
import skadistats.clarity.wire.common.proto.Demo.CDemoFileInfo;
import skadistats.clarity.wire.common.proto.Demo.CGameInfo.CDotaGameInfo;
import skadistats.clarity.wire.common.proto.Demo.CGameInfo.CDotaGameInfo.CHeroSelectEvent;
import skadistats.clarity.wire.common.proto.Demo.CGameInfo.CDotaGameInfo.CPlayerInfo;

public class Info {
    public Map<String, Object> getInfo(String file) throws Exception {
        CDemoFileInfo info = Clarity.infoForFile(file);
        CDotaGameInfo gameInfo = info.getGameInfo().getDota();
        HashMap<String, Object> map = new HashMap<String, Object>();

        for (Map.Entry<FieldDescriptor, Object> entry : gameInfo.getAllFields().entrySet()) {
            FieldDescriptor k = entry.getKey();
            Object v = entry.getValue();

            if (v instanceof List) {
                v = disentangleList((List) v);
            } else {
                v = v.toString();
            }

            map.put(k.getFullName().replace('.', '-').replaceAll("CGameInfo-CDotaGameInfo-", ""), v);
        }

        return map;
    }

    private HashMap<String, ArrayList<HashMap<String, Object>>> disentangleList(List list) throws Exception {

        ArrayList<HashMap<String, Object>> resultList = new ArrayList<HashMap<String, Object>>();
        HashMap<String, Object> temp;
        for (Object item : (List) list) {
            temp = new HashMap<String, Object>();
            if (item instanceof CPlayerInfo) {
                for (Map.Entry<FieldDescriptor, Object> playerEntry : ((CPlayerInfo) item).getAllFields().entrySet()) {
                    FieldDescriptor k = playerEntry.getKey();
                    Object v = playerEntry.getValue();
                    temp.put(k.getFullName().replace('.', '-').replaceAll("CGameInfo-CDotaGameInfo-CPlayerInfo-", ""),
                            v);
                }
            } else if (item instanceof CHeroSelectEvent) {
                for (Map.Entry<FieldDescriptor, Object> playerEntry : ((CHeroSelectEvent) item).getAllFields()
                        .entrySet()) {
                    FieldDescriptor k = playerEntry.getKey();
                    Object v = playerEntry.getValue();
                    temp.put(k.getFullName().replace('.', '-').replaceAll("CGameInfo-CDotaGameInfo-CHeroSelectEvent-",
                            ""), v);
                }
            } else {
                throw new Exception("Unknown class in game info: " + item.getClass().toString());
            }
            resultList.add(temp);
        }

        String key = (list.get(0) instanceof CPlayerInfo) ? "PlayerInfo" : "HeroSelectEvert";

        HashMap<String, ArrayList<HashMap<String, Object>>> res = new HashMap<String, ArrayList<HashMap<String, Object>>>();
        res.put(key, resultList);
        return res;
    }
}
