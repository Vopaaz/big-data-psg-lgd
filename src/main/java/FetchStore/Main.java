package FetchStore;

public class Main {
    public static void main(String[] args) {
        try {
            String seqNumber = args[0];
            int totalNumber = Integer.parseInt(args[1]);
            int batchSize = Integer.parseInt(args[2]);
            ValveAPI api = new ValveAPI("config.yml");
            api.getMultipleMatchesBySeqNum(seqNumber, totalNumber, batchSize);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
