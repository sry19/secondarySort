import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlightSortComparator extends WritableComparator {
    public FlightSortComparator() {
        super(FlightKey.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        FlightKey flightKey1 = (FlightKey) wc1;
        FlightKey flightKey2 = (FlightKey) wc2;
        return flightKey1.compareTo(flightKey2);
    }
}
