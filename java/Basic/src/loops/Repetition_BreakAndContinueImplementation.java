package loops;

public class Repetition_BreakAndContinueImplementation {
    public static void main(String[] args) {
        var nilai = 5;

        // contoh penerapan break
//        while (true) {
//            System.out.println("Now, we have number " + nilai);
//            nilai++;
//
//            if(nilai > 10) {
//                break;
//            }
//        }

        // contoh penerapan continue
        for(var number = 1;number <= 10;number++) {
            if (number % 2 == 1) {
                continue;
            }
            System.out.println("Now, we have number " + number);
        }
    }
}
