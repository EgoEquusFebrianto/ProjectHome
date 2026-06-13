package loops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Repetition_AnotherMethod {
    public static void main(String[] args) {
        List<Integer> num = new ArrayList<>(Arrays.asList(10, 20, 30 ,40, 50));

        // iterator loop -> Kelebihan, aman untuk manipulasi koleksi. Kekurangan, Lebih kompleks dibandingkan for-each
        Iterator<Integer> it = num.iterator();

        while(it.hasNext()) {
            System.out.println("it = " + it);
            Integer val = it.next();
        }
        System.out.println(num + "\n");

        // Stream API -> kelebihan, Lebih deklaratif. Kekurangan, Tidak bisa menggunakan break atau continue
        // model 1
        num.forEach(System.out::println);
        System.out.println();

        // model 2
        num.forEach(value -> {
            var res = value + 1;
            System.out.println(res);
        });
        System.out.println();

        // parallel Stream -> Kelebihan, Lebih cepat di CPU multicore. Kekurangan, Hasil tidak berurutan
        num.parallelStream().forEach(System.out::println);
    }
}
