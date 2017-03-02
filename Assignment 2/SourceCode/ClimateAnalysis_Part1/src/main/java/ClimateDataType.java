import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ps on 2/1/17.
 */
// Class objects used as accumulation data structures
public class ClimateDataType implements Writable {

    double stationMinTemperature;

    double stationMaxTemperature;

    // boolean variable true if record has TMAX, false if record has TMIN
    boolean minOrMax;

    double getStationMinTemperature() {
        return stationMinTemperature;
    }

    double getStationMaxTemperature() {
        return stationMaxTemperature;
    }

    boolean isMinOrMax() {
        return minOrMax;
    }

     ClimateDataType(double stationMinTemperature, double stationMaxTemperature, boolean minOrMax) {
        this.stationMinTemperature = stationMinTemperature;
        this.stationMaxTemperature = stationMaxTemperature;
        this.minOrMax=minOrMax;

    }
    public ClimateDataType(){

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(getStationMinTemperature());
        dataOutput.writeDouble(getStationMaxTemperature());
        dataOutput.writeBoolean(minOrMax);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.stationMinTemperature = dataInput.readDouble();
        this.stationMaxTemperature = dataInput.readDouble();
        this.minOrMax = dataInput.readBoolean();
    }
}
