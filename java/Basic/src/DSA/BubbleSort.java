package DSA;

import java.util.StringJoiner;

public class BubbleSort {
    public static void main(String[] args) {
        int[] dataArray = { 7, 2, 5, 1, 4, 3, 6 };

        for (int i = 0; i < dataArray.length - 1; i++) {
            for (int j = 0; j < dataArray.length - 1 - i; j++) {
                if (dataArray[j] > dataArray[j + 1]) {
                    int temp = dataArray[j];
                    dataArray[j] = dataArray[j + 1];
                    dataArray[j + 1] = temp;
                }
            }
        }

        for (int i : dataArray) {
            System.out.printf("%d, ", i);
        }

    }
}
