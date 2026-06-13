package javaLambda;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class AnotherJavaLambdaImpl {
    public static void main(String[] args) {
        // Consumer dapat digunakan bila hanya untuk menerima data
        Consumer<String> consumer = value -> System.out.println(value);
        consumer.accept("Febrian");

        // Function<T, R>
        Function<String, Integer> functionLength = value -> value.length();
        System.out.println(functionLength.apply("Febrian"));

        // Predicate<T> return: Biasanya digunakan untuk mengecek
        Predicate<String> predicate = value -> value.isBlank();
        System.out.println(predicate.test("Febrian"));

        // Supplier, untuk mengembalikan resouce tanpa parameter
        Supplier<String> supplier = () -> "Febrian";
        System.out.println(supplier.get());

    }
}
