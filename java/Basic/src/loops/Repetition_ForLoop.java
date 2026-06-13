package loops;

public class Repetition_ForLoop {
    public static void main(String[] args) {
        // For Loop Iteration, structured
        // for (init_statement ; Condition : post_statement) { Code/Syntax here }

        var counter = 5;

        // 1. Perulangan tanpa kondisi
//        for (;;) {
//            System.out.println("This is Infinity For Loop");
//        }

        // 2. Perulangan dengan kondisi, hanya dengan kondisi
//        for (;counter <= 10;) {
//            System.out.println("Now, we have number " + counter);
//            counter++;
//        }
        // 3. Perulangan dengan init statement dan kondisi
//        for (var nilai = 5; nilai <= 7;) {
//            System.out.println("Now, we have number " + nilai);
//            nilai++;
//        }
        // 4. Perulangan complete
        for(var nilai = 5; nilai <= 10; nilai++) {
            System.out.println("Now, we have number " + nilai);
        }
    }
}
