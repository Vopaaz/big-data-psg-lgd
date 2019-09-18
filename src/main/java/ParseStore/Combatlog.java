package ParseStore;

import java.util.ArrayList;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import skadistats.clarity.model.CombatLogEntry;
import skadistats.clarity.processor.gameevents.OnCombatLogEntry;
import skadistats.clarity.processor.runner.SimpleRunner;
import skadistats.clarity.source.MappedFileSource;

public class Combatlog {

    private final Logger log = LoggerFactory.getLogger(Main.class.getPackage().getClass());

    private ArrayList<Document> eventList = new ArrayList<Document>();

    private String compileName(String attackerName, boolean isIllusion) {
        return (attackerName != null ? attackerName + (isIllusion ? " (illusion)" : "") : "UNKNOWN");
    }

    private String getAttackerNameCompiled(CombatLogEntry cle) {
        return compileName(cle.getAttackerName(), cle.isAttackerIllusion());
    }

    private String getTargetNameCompiled(CombatLogEntry cle) {
        return compileName(cle.getTargetName(), cle.isTargetIllusion());
    }

    @OnCombatLogEntry
    public void onCombatLogEntry(CombatLogEntry cle) {
        float time = cle.getTimestamp();

        Document temp;
        switch (cle.getType()) {
        case DOTA_COMBATLOG_DAMAGE:
            temp = new Document("time", time).append("type", "damage").append("attacker", getAttackerNameCompiled(cle))
                    .append("target", getTargetNameCompiled(cle)).append("inflictor", cle.getInflictorName())
                    .append("damage", cle.getValue()).append("before_hp", cle.getHealth() + cle.getValue())
                    .append("after_hp", cle.getHealth());
            eventList.add(temp);
            break;
        case DOTA_COMBATLOG_HEAL:
            temp = new Document("time", time).append("type", "heal").append("healer", getAttackerNameCompiled(cle))
                    .append("inflictor", cle.getInflictorName()).append("target", getTargetNameCompiled(cle))
                    .append("health", cle.getValue()).append("before_hp", cle.getHealth() - cle.getValue())
                    .append("after_hp", cle.getHealth());
            eventList.add(temp);
            break;
        case DOTA_COMBATLOG_MODIFIER_ADD:
            temp = new Document("time", time).append("type", "add_buff").append("target", getTargetNameCompiled(cle))
                    .append("inflictor", cle.getInflictorName()).append("attacker", getAttackerNameCompiled(cle));
            eventList.add(temp);
            break;
        case DOTA_COMBATLOG_MODIFIER_REMOVE:
            temp = new Document("time", time).append("type", "lose_buff").append("target", getTargetNameCompiled(cle))
                    .append("inflictor", cle.getInflictorName());
            eventList.add(temp);
            break;
        case DOTA_COMBATLOG_DEATH:
            temp = new Document("time", time).append("type", "death").append("target", getTargetNameCompiled(cle))
                    .append("killer", getAttackerNameCompiled(cle));
            eventList.add(temp);
            break;
        case DOTA_COMBATLOG_ABILITY:

            String abilityType;
            if (cle.isAbilityToggleOn()) {
                abilityType = "toggle_on";
            } else if (cle.isAbilityToggleOff()) {
                abilityType = "toggle_off";
            } else {
                abilityType = "cast";
            }

            temp = new Document("time", time).append("type", "ability").append("ability_type", abilityType)
                    .append("level", cle.getAbilityLevel()).append("target", getTargetNameCompiled(cle));
            eventList.add(temp);
            break;
        case DOTA_COMBATLOG_ITEM:
            temp = new Document("time", time).append("type", "item").append("user", getAttackerNameCompiled(cle))
                    .append("item", cle.getInflictorName());
            eventList.add(temp);
            break;
        case DOTA_COMBATLOG_GOLD:
            temp = new Document("time", time).append("type", "gold").append("target", getTargetNameCompiled(cle))
                    .append("change", cle.getValue());
            eventList.add(temp);
            break;
        case DOTA_COMBATLOG_GAME_STATE:
            // log.info("{} game state is now {}", time, cle.getValue());
            // The game state seems to be not meaningful.
            break;
        case DOTA_COMBATLOG_XP:
            temp = new Document("time", time).append("type", "XP").append("target", getTargetNameCompiled(cle))
                    .append("xp", cle.getValue());
            eventList.add(temp);
            break;
        case DOTA_COMBATLOG_PURCHASE:
            temp = new Document("time", time).append("type", "purchase").append("target", getTargetNameCompiled(cle))
                    .append("item", cle.getValueName());
            eventList.add(temp);
            break;
        case DOTA_COMBATLOG_BUYBACK:
            temp = new Document("time", time).append("type", "buyback").append("slot", cle.getValue());
            eventList.add(temp);
            break;

        default:
            break;
        }
    }

    public ArrayList<Document> getEvents(String file) throws Exception {
        new SimpleRunner(new MappedFileSource(file)).runWith(this);
        return eventList;
    }
}