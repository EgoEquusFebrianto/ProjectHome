package operations;

public class Operations_Math {
    public static void main(String[] args) {
        byte a = 100;
        byte b = 9;
        byte c = 0;

        // Operasi biasa
        System.out.println(a + b);
        System.out.println(a - b);
        System.out.println(a * b);
        System.out.println(a / b);
        System.out.println(a % b);

        // Augmented Assignment
        c += 10;
        System.out.println();
        System.out.println(c);

        c -= 5;
        System.out.println(c);

        c *= 2;
        System.out.println(c);

        c /= 0.5;
        System.out.println(c);

        c %= 8;
        System.out.println(c);
        System.out.println();

        // Unary Operator
        c++; // ini sama artinya dengan c = c + 1 atau c += 1
        System.out.println(c);

        c--;
        System.out.println(c);

        // ada juga pemberian "+" dan "-" diawal.
        // untuk boolean ada penambahan "!" diawal untuk kebalikan dari nilai.
        int test = 13;
        System.out.println(test);

        ++test;
        System.out.println(test);

        --test;
        System.out.println(test);
    }
}
