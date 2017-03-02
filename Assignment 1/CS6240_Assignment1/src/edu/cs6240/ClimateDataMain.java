package edu.cs6240;
import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by ps on 1/23/17.
 */

public class ClimateDataMain{
    static List<String> listOfData = new ArrayList<>();
    public static void main(String args[]) throws Exception {
        FileInputStream csvFile = new FileInputStream("/home/ps/IdeaProjects/CS6240_Assignment1/src/1912.csv.gz");
        GZIPInputStream unzippedCsvFile = new GZIPInputStream(csvFile);
        csvFileLoader(unzippedCsvFile);


        // trying to input more number of cores to check failure
//        Scanner x= new Scanner(System.in);
//        System.out.println("Please enter number of threads eligible for your processor");
//        int noOfThreads = x.nextInt();

        // gives number of cores on your processor
        int noOfThreads = Runtime.getRuntime().availableProcessors();

        new ClimateData_Sequential();
        new CourseLockAverage(noOfThreads);
        new FineLockAverage(noOfThreads);
        new NoLockAverage(noOfThreads);
        new NoSharedAverage(noOfThreads);

    }

    private static void csvFileLoader(GZIPInputStream csvFile) {
        String line = "";
        String separator = ",";
        try (BufferedReader br = new BufferedReader(new InputStreamReader(csvFile))) {
            while ((line = br.readLine()) != null) {
                listOfData.add(line);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
