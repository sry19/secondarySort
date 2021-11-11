import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FlightKey implements WritableComparable<FlightKey> {

    private Text airlineName;
    private IntWritable month;

    public FlightKey() {
        airlineName = new Text();
        month = new IntWritable();
    }

    public static FlightKey createFlightKey(String airlineName, String month) {
        if (Objects.equals(airlineName, "")) {
            return null;
        }
        try {
            int m = Integer.parseInt(month);
            FlightKey flightKey = new FlightKey();
            flightKey.setAirlineName(new Text(airlineName));
            flightKey.setMonth(new IntWritable(m));
            return flightKey;
        } catch (Exception e) {
            return null;
        }

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.airlineName.write(dataOutput);
        this.month.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.airlineName.readFields(dataInput);
        this.month.readFields(dataInput);
    }

    public Text getAirlineName() {
        return airlineName;
    }

    public void setAirlineName(Text airlineName) {
        this.airlineName = airlineName;
    }

    public IntWritable getMonth() {
        return month;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlightKey flightKey = (FlightKey) o;
        return airlineName.equals(flightKey.airlineName) && month.equals(flightKey.month);
    }

    @Override
    public int hashCode() {
        return Objects.hash(airlineName);
    }

    @Override
    public int compareTo(FlightKey o) {
        int result1 = this.airlineName.toString().compareTo(o.airlineName.toString());
        if (result1 == 0) {
            Integer month1 = this.month.get();
            Integer month2 = o.month.get();
            return month1.compareTo(month2);
        }
        return result1;
    }
}
