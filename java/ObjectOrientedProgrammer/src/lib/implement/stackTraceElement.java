package lib.implement;

public class stackTraceElement {
    public static void main(String[] args) {
        sampleError();
//        try {
//            String[] names = {"Febrianto", "Kudadiri", "Elements"};
//            System.out.println(names[5]);
//        } catch (Throwable throwable) {
//            StackTraceElement[] stackTraces = throwable.getStackTrace();
//
////            for (var value : stackTraces) {
////                System.out.println(value);
////            }
//            throwable.printStackTrace();
//        }
    }

    static void sampleError() {
        try {
            String[] names = {"Febrianto", "Kudadiri", "Elements"};
            System.out.println(names[5]);
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }
}