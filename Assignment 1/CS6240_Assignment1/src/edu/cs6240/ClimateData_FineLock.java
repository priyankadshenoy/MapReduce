package edu.cs6240;

import java.util.*;

import static edu.cs6240.ClimateDataMain.listOfData;

/**
 * Created by ps on 1/26/17.
 */
public class ClimateData_FineLock extends Thread{
    private int start;
    private int size;

    ClimateData_FineLock() {
    }

    ClimateData_FineLock(int start, int size) {
        this.start= start;
        this.size = size;
    }

    @Override
    public void run() {
        for (int i = start; i < size ; i++) {
            FineLockAverage.updateData(i);
        }
    }

    // creating array of type ClimateData_FineLock according to number of cores in processor
    ClimateData_FineLock[] createArray(int noOfThreads) {
        ClimateData_FineLock[] fc= new ClimateData_FineLock[noOfThreads];
        return fc;

    }
}

class FineLockAverage {

    private static Map<String, AccumulationDataStructure> listOfStation;

    FineLockAverage(int noOfThreads) throws Exception{

        // array which contains all time's of execution ; used ahead for sorting and getting min max and average
        List<Double> minMaxAvg = new ArrayList<>();

        double totalSumTmax = 0.0;

        for (int i = 0; i < 10; i++) {

            listOfStation = new HashMap<>();

            double t_start = System.currentTimeMillis();

            int size_0 = 0;
            int size_1 = listOfData.size() / noOfThreads;

            // creating array of threads according to the number of cores
            ClimateData_FineLock cclock = new ClimateData_FineLock();
            ClimateData_FineLock[] arraycclock = cclock.createArray(noOfThreads);

            for (int j = 0; j < noOfThreads; j++) {

                // setting the start and end size of file for nth thread
                arraycclock[j] = new ClimateData_FineLock(size_0, size_1);
                arraycclock[j].start();

                //setting start and end of size for (n+1)th thread

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
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            double t_end = System.currentTimeMillis();
            totalSumTmax += (t_end - t_start);
            minMaxAvg.add(t_end - t_start);
        }

        Collections.sort(minMaxAvg);

        System.out.println("Fine Lock Data");
        System.out.println("Avg run" + totalSumTmax / 10);
        System.out.println("Min run" + minMaxAvg.get(0));
        System.out.println("Max run" + minMaxAvg.get(9));
        System.out.println();
        //this.displayData(listOfStation);

    }

    static void updateData(int index) {

        String record = listOfData.get(index);
        if (record.contains("TMAX")) {

            // since the input data is csv, splitting it according to commas and we need only ID and
            // Max temperature which is done in the following lines
            String[] stationDetails = record.split(",");
            String stationDetailsID = stationDetails[0];
            String stationDetailsValue = stationDetails[3];

            // if station ID already present then update temperature
            if (listOfStation.containsKey(stationDetailsID)){
                listOfStation.get(stationDetailsID).updateMaxTempSync(Double.parseDouble(stationDetailsValue));
            }

            // if station ID is new, create new record
            else
            {
                AccumulationDataStructure station = new AccumulationDataStructure(Double.parseDouble(stationDetailsValue), 1);
                // for FineLock we need to lock individual id and value of the map. Locking at this position eg id:101 and value: blah
                // is locked until execution of method. Other threads can access different different id and pairs simultaneously.
                // I though of using Concurrent HashMap, but that would be putting a lock on my entire data structure, which would be very
                // similar to course lock which is not the requirement
                synchronized (station) {
                    if(listOfStation.containsKey(stationDetailsID))
                    {
                        listOfStation.get(stationDetailsID).updateMaxTempSync(Double.parseDouble(stationDetailsValue));
                    }
                        else
                    {
                        AccumulationDataStructure.fibonacci(17);
                        listOfStation.put(stationDetailsID,  station);
                    }

                }
            }

        }
    }

    // displaying average data if necessary
    private void displayData(Map<String, AccumulationDataStructure> listOfStation) {
        for(String record : listOfStation.keySet()){
            System.out.println(listOfStation.get(record).getAvg());
        }

    }
}




