package methods;

public class Method_Recursive {
    // tanpa recursive
    static int factorial(int value) {
        var hasil = 1;
        for (var nilai = 1 ; nilai <= value; nilai++) {
            hasil *= nilai;
        }
        return hasil;
    }

    // dengan recursive
    static int factorialRec(int value) {
        if (value == 1) {
            return 1;
        } else {
            return value * factorialRec(value - 1);
        }
    }

    public static void main(String[] args) {
        System.out.println(factorial(5));
        System.out.println(factorialRec(5));
    }

}
