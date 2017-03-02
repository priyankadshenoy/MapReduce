package edu.cs6240;
import java.util.*;

import static edu.cs6240.ClimateDataMain.listOfData;

/**
 * Created by ps on 1/27/17.
 */
class ClimateData_Sequential {

    ClimateData_Sequential() {

        // array which contains all time's of execution ; used ahead for sorting and getting min max and average
        List<Double> minMaxAvg = new ArrayList<>();
        double totalSumTmax= 0.0;

        for (int i = 0; i < 10; i++) {
            Map<String, AccumulationDataStructure> listOfStation = new HashMap<String, AccumulationDataStructure>();

            double t_start = System.currentTimeMillis();

            // since the input data is csv, splitting it according to commas and we need only ID and
            // Max temperature which is done in the following lines
            for (String station : listOfData) {
                if (station.contains("TMAX")) {
                    String[] stationDetails = station.split(",");
                    String stationDetailsID = stationDetails[0];
                    String stationDetailsValue = stationDetails[3];

                    if (listOfStation.containsKey(stationDetailsID))
                        listOfStation.get(stationDetailsID).updateMaxTemp(Double.parseDouble(stationDetailsValue));

                    else {
                        AccumulationDataStructure newStation = new AccumulationDataStructure(Double.parseDouble(stationDetailsValue), 1);
                        AccumulationDataStructure.fibonacci(17);
                        listOfStation.put(stationDetailsID, newStation);
                    }
                }
            }

            double t_end = System.currentTimeMillis();
            minMaxAvg.add(t_end-t_start);
            totalSumTmax += (t_end - t_start);
            //this.displayData(listOfStation);

        }

        // sorting times of each run
        Collections.sort(minMaxAvg);

        System.out.println("Sequential Data");
        System.out.println("Avg run "+ totalSumTmax/10);
        System.out.println("Min run "+ minMaxAvg.get(0));
        System.out.println("Max run "+ minMaxAvg.get(9));
        System.out.println();


    }

    // displaying average data if necessary
    private void displayData(Map<String, AccumulationDataStructure> listOfStation) {
        for(String record : listOfStation.keySet()){
            System.out.println(listOfStation.get(record).getAvg());
        }

    }
}
