package javaStandardClasses.readData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class csvReaderBasic {
    public static void main(String[] args) {
        File fileDir = new File("dataset/example.csv");

        try (BufferedReader reader = new BufferedReader(new FileReader(fileDir))) {
            String line;

            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
