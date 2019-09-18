package ParseStore;

import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ParseStore.helper.OnEntityDied;
import ParseStore.helper.OnEntitySpawned;
import skadistats.clarity.event.Insert;
import skadistats.clarity.model.Entity;
import skadistats.clarity.processor.runner.Context;
import skadistats.clarity.processor.runner.SimpleRunner;
import skadistats.clarity.source.MappedFileSource;

public class Lifestate {

    private final Logger log = LoggerFactory.getLogger(Lifestate.class);

    private ArrayList<HashMap<String, Object>> eventList = new ArrayList<HashMap<String, Object>>();
    private HashMap<String, Object> tmpEvent;

    @Insert
    private Context ctx;

    @OnEntitySpawned
    public void onSpawned(Entity e) {
        tmpEvent = new HashMap<String, Object>();
        tmpEvent.put("tick", ctx.getTick());
        tmpEvent.put("object", e.getDtClass().getDtName());
        tmpEvent.put("type", "spawn");
        eventList.add(tmpEvent);
    }

    @OnEntityDied
    public void onDied(Entity e) {
        tmpEvent = new HashMap<String, Object>();
        tmpEvent.put("tick", ctx.getTick());
        tmpEvent.put("object", e.getDtClass().getDtName());
        tmpEvent.put("type", "die");
        eventList.add(tmpEvent);
    }

    public ArrayList<HashMap<String, Object>> getStates(String file) throws Exception {
        new SimpleRunner(new MappedFileSource(file)).runWith(this);
        return eventList;
    }

}
