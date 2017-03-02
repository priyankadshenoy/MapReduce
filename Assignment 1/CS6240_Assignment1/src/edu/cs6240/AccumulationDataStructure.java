package edu.cs6240;

/**
 * Created by ps on 1/26/17.
 */

    // I have defined this data structure as a Hashmap with id as station id
    // and value as object of AccumulationDataStructure class which has sum and count as class variable
    // avg is used to update whenever there is a change in data structure
class AccumulationDataStructure {
    private double sumOfMaxTemp;
    private int count;
    private double avg;

    AccumulationDataStructure(double sumOfMaxTemp, int count){
        this.sumOfMaxTemp = sumOfMaxTemp;
        this.count = count;
        this.avg = this.sumOfMaxTemp / this.count;
    }


    int getCount() {

        return count;
    }

    double getSumOfMaxTemp() {

        return sumOfMaxTemp;
    }

    void updateMaxTemp(double temp){
        fibonacci(17);
        this.sumOfMaxTemp = sumOfMaxTemp+ temp;
        count++;
        this.avg = sumOfMaxTemp / count ;
    }
    // used for fine lock data
    synchronized void updateMaxTempSync(double temp){
        fibonacci(17);
        this.sumOfMaxTemp = temp + this.sumOfMaxTemp;
        count++;
        this.avg = sumOfMaxTemp / count;
    }

    void increaseTempCount(double sumOfMaxTemp, int count){
        fibonacci(17);
        this.sumOfMaxTemp += sumOfMaxTemp;
        this.count += count;
        this.avg = this.sumOfMaxTemp / this.count;
    }

    public static void fibonacci(int value){
        int x = 0, y = 1, z = 1;
        for (int i = 0; i < value; i++) {
            x = y;
            y = z;
            z = x + y;
        }
    }

    double getAvg() {
        return avg;
    }

}
