package ParseStore;

public class Main {
    public static void main(String[] args) {
        try {
            String file = args[0];
            new ParseStoreExecutor().parseFileStoreMongo(file, "test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}