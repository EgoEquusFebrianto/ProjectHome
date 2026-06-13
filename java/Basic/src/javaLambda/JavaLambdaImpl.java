package javaLambda;

public class JavaLambdaImpl {
    public static void main(String[] args) {
        // Dengan Block
        SimpleAction simpleAction1 = (String name) -> {
            String res = "Hello, " + name;
            return res;
        };

        // Tanpa Block
        SimpleAction simpleAction2 = name -> "Hello, " + name;

        System.out.println(simpleAction1.action("Febrian"));
        System.out.println(simpleAction2.action("Febrian"));
    }
}
