package edu.cs6240;

import java.util.*;

import static edu.cs6240.ClimateDataMain.listOfData;

/**
 * Created by ps on 1/28/17.
 */
public class ClimateData_NoShared extends Thread {


    private int start;
    private int size;

    ClimateData_NoShared() {

    }

    //Accumulated data structure for each thread
    private Map<String, AccumulationDataStructure> noSharingData = new HashMap<>();

    // method which returns data for current thread been processed
    Map<String, AccumulationDataStructure> getNoSharingData() {
        return noSharingData;
    }

    ClimateData_NoShared(int start, int size) {
        this.start = start;
        this.size = size;
    }

    @Override
    public void run() {

        for (int i = start; i < size; i++) {

            String record = listOfData.get(i);

            // since the input data is csv, splitting it according to commas and we need only ID and
            // Max temperature which is done in the following lines
            if (record.contains("TMAX")) {
                String[] stationDetails = record.split(",");
                String stationDetailsID = stationDetails[0];
                String stationDetailsValue = stationDetails[3];

                if (noSharingData.containsKey(stationDetailsID)) {
                    noSharingData.get(stationDetailsID).updateMaxTemp(Double.parseDouble(stationDetailsValue));
                }

                else {
                    AccumulationDataStructure station = new AccumulationDataStructure(Double.parseDouble(stationDetailsValue), 1);
                    noSharingData.put(stationDetailsID, station);
                }

            }
        }
    }

    // creating array of type ClimateData_NoShared according to number of cores in processor
    ClimateData_NoShared[] createArray(int noOfThreads) {
        ClimateData_NoShared[] nc= new ClimateData_NoShared[noOfThreads];
        return nc;
    }
}

class NoSharedAverage {

    private static Map<String, AccumulationDataStructure> listOfStation;

    NoSharedAverage(int noOfThreads) {

        // array which contains all time's of execution ; used ahead for sorting and getting min max and average
        List<Double> minMaxAvg = new ArrayList<>();

        double totalSumTmax = 0.0;

        for (int i = 0; i < 10; i++) {

            listOfStation = new HashMap<>();

            double t_start = System.currentTimeMillis();

            int size_0 = 0;
            int size_1 = listOfData.size() / noOfThreads;

            // creating array of threads according to the number of cores
            ClimateData_NoShared cclock = new ClimateData_NoShared();
            ClimateData_NoShared[] arraycclock = cclock.createArray(noOfThreads);


            for (int j = 0; j < noOfThreads; j++) {
                // setting the start and end size of file for first thread
                arraycclock[j] = new ClimateData_NoShared(size_0, size_1);
                arraycclock[j].start();


                //setting start and end of size for next thread
                size_0 = size_1;


                // if the records in file is odd, we need to make sure last record is parsed as well.
                // hence setting the last chunk of data to be size of data (Got NullPointerException hence added this condition)
                if (j == noOfThreads - 2)
                    size_1 = listOfData.size();
                else
                    size_1 += listOfData.size() / noOfThreads;
            }

                try {
                    for (int k = 0; k < noOfThreads; k++) {
                        arraycclock[k].join();
                    }
                }
                    catch(InterruptedException e){
                        e.printStackTrace();
                    }

            // since in no shared we have different accumulation data structures, we need to add the data to our original
            // accumulation data structure
            // adding all values of first thread to original accumulation data structure
            AccumulationDataStructure.fibonacci(17);
            listOfStation.putAll(arraycclock[0].getNoSharingData());

            // iterating through the rest of the threads to check if any station ID is present, if it is
            // we update temperature, else add new record
            for(int l = 1; l< noOfThreads ; l++) {

                for (String s : arraycclock[l].getNoSharingData().keySet()) {

                    //
                    if (listOfStation.containsKey(s)) {

                        double tmax_increase = arraycclock[l].getNoSharingData().get(s).getSumOfMaxTemp();
                        int count_increase = arraycclock[l].getNoSharingData().get(s).getCount();
                        listOfStation.get(s).increaseTempCount(tmax_increase, count_increase);

                    } else {

                        AccumulationDataStructure.fibonacci(17);
                        listOfStation.put(s, arraycclock[l].getNoSharingData().get(s));
                    }
                }
            }

            long t_end = System.currentTimeMillis();
            minMaxAvg.add(t_end - t_start);
            totalSumTmax += (t_end - t_start);
        }

        Collections.sort(minMaxAvg);

        System.out.println("No Shared Data");
        System.out.println("Avg run" + totalSumTmax / 10);
        System.out.println("Min run" + minMaxAvg.get(0));
        System.out.println("Max run" + minMaxAvg.get(9));
        System.out.println();
        //this.displayData(listOfStation);
    }

    private void displayData(Map<String, AccumulationDataStructure> listOfStation) {
        for(String record : listOfStation.keySet()){
            System.out.println(listOfStation.get(record).getAvg());
        }

    }

}
