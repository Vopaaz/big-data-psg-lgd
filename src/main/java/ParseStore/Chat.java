package ParseStore;

import java.util.ArrayList;
import java.util.HashMap;

import skadistats.clarity.processor.reader.OnMessage;
import skadistats.clarity.processor.runner.Context;
import skadistats.clarity.processor.runner.SimpleRunner;
import skadistats.clarity.source.MappedFileSource;
import skadistats.clarity.source.Source;
import skadistats.clarity.wire.s2.proto.S2UserMessages;

public class Chat {
    private ArrayList<HashMap<String, String>> chatList = new ArrayList<HashMap<String, String>>();
    private HashMap<String, String> tmpMessage;

    @OnMessage(S2UserMessages.CUserMessageSayText2.class)
    public void onMessage(Context ctx, S2UserMessages.CUserMessageSayText2 message) {
        tmpMessage = new HashMap<String, String>();
        tmpMessage.put("sender", message.getParam1());
        tmpMessage.put("message", message.getParam2());
        chatList.add(tmpMessage);
    }

    public ArrayList<HashMap<String, String>> getChat(String file) throws Exception {
        Source source = new MappedFileSource(file);
        SimpleRunner runner = new SimpleRunner(source);
        runner.runWith(this);
        return chatList;
    }

}
