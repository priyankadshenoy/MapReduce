import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ps on 2/4/17.
 */
// Class objects used as accumulation data structures
public class ClimateDataType implements Writable {

    double stationMinTemperature;

    double stationMaxTemperature;

    int countMin;

    int countMax;


    ClimateDataType(double stationMinTemperature, double stationMaxTemperature, int countMin, int countMax) {
        this.stationMinTemperature = stationMinTemperature;
        this.stationMaxTemperature = stationMaxTemperature;
        this.countMin = countMin;
        this.countMax = countMax;
    }

    void updateMax(double maxT){
        stationMaxTemperature +=maxT;
        countMax ++;
    }


    int getCountMin() {
        return countMin;
    }

    int getCountMax() {
        return countMax;
    }

    double getStationMinTemperature() {
        return stationMinTemperature;
    }

    double getStationMaxTemperature() {
        return stationMaxTemperature;
    }

    public ClimateDataType(){

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(stationMinTemperature);
        dataOutput.writeDouble(stationMaxTemperature);
        dataOutput.writeInt(countMin);
        dataOutput.writeInt(countMax);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.stationMinTemperature = dataInput.readDouble();
        this.stationMaxTemperature = dataInput.readDouble();
        this.countMin=dataInput.readInt();
        this.countMax=dataInput.readInt();
    }

    void updateMin(double temp) {
         stationMinTemperature += temp;
         countMin ++;
    }
}

