package lib.algorithm;

public class staticBlock {
    public static final int PROCESSORS;

    static {
        System.out.println("This Command Will Running First");
        PROCESSORS = Runtime.getRuntime().availableProcessors();
    }
}