package loops;

public class Repetition_ForEach {
    public static void main(String[] args) {
        int[] numbers = {
                1, 4, 6, 9, 11, 15
        };

        // tanpa for each
//        for (var num = 0; num < numbers.length; num++) {
//            System.out.println("Now, we have number " + numbers[num]);
//        }

        // dengan for each
        for (var value : numbers) {
            System.out.println("Now, we have number " + value);
        }
    }
}
