import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ps on 2/7/17.
 */
public class CompositeKey implements WritableComparable {

    String stationId;
    Integer year;

    String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, stationId);
        WritableUtils.writeVInt(dataOutput, year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        stationId = WritableUtils.readString(dataInput);
        year = WritableUtils.readVInt(dataInput);
    }

    CompositeKey(String stationId, int year){
        this.stationId = stationId;
        this.year = year;
    }

    CompositeKey(){

    }

    //overridden method which compares values of station id and year
    // if station id's are equal
    // returns < 0 if year of current object < parameter  (>0 for vice versa)
    // if station id's are not equal
    // returns < 0 if stationid if current object < parameter  (>0 for vice versa)
    @Override
    public int compareTo(Object cKey) {
        CompositeKey comp = (CompositeKey)cKey;
        int stationValue = this.stationId.compareTo(comp.stationId);
        int yearValue = this.year.compareTo(comp.year);
        if(this.stationId.equals(comp.stationId))
            return yearValue;
        else
            return stationValue;
    }


}
