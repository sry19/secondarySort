import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Secondary {

    public static class SecondaryMapper extends Mapper<Object, Text, FlightKey, Text> {

    }
}
