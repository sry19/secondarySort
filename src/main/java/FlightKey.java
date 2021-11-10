import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FlightKey implements Writable {

    private Text airlineName;
    private IntWritable month;

    public FlightKey() {
        airlineName = new Text();
        month = new IntWritable();
    }

    public FlightKey createFlightKey(String airlineName, String month) {
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
}
