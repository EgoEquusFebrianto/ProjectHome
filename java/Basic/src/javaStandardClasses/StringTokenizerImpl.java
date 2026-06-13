package javaStandardClasses;

import java.util.StringTokenizer;

public class StringTokenizerImpl {
    public static void main(String[] args) {
        // ini adalah solusi lazy dan hemat memory dari split
        String name = "The Hash Slinging Slicer";
        StringTokenizer stringTokenizer = new StringTokenizer(name, ", ");

        while(stringTokenizer.hasMoreTokens()) {
            String res = stringTokenizer.nextToken();
            System.out.println(res);
        }

    }
}
