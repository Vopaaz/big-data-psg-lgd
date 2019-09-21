package ParseReplay;

public class Main {
    public static void main(String[] args) {
        try {
            String file = args[0];
            new ParseReplayExecutor().parseFileStoreMongo(file, "test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
