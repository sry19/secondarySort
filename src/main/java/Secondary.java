import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Secondary {

    public static class SecondaryMapper extends Mapper<Object, Text, FlightKey, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();
            if (input.equals("")) {
                return;
            }
            // split lines by comma
            String reg = ",(?!\\s)";
            String[] res = input.split(reg);
            if (res[2] == "" || res[7] == "" || res[37] == "") {
                return;
            }
            FlightKey flightKey = FlightKey.createFlightKey(res[7],res[2]);
            if (flightKey == null) {
                return;
            }
            context.write(flightKey, new Text(res[37]));
        }
    }

    public static class SecondaryPartitioner extends Partitioner<FlightKey, Text> {
        @Override
        public int getPartition(FlightKey flightKey, Text delay, int numPartitions) {
            return flightKey.hashCode() % numPartitions;
        }
    }



    public static class SecondaryReducer extends Reducer<FlightKey, Text, Text, Text> {
        public void reduce(FlightKey flightKey, Text value, Text outputKey, Text outputValue) {

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: flight delay <in> [<in>...] <out>");
            System.exit(2);
        }
        // job1
        Job job = Job.getInstance(conf, "flight delay");
        job.setJarByClass(Secondary.class);
        job.setMapperClass(SecondaryMapper.class);
        job.setReducerClass(SecondaryReducer.class);
        job.setPartitionerClass(SecondaryPartitioner.class);
        //job.setOutputValueClass(FlightData.class);
        job.setMapOutputKeyClass(FlightKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setGroupingComparatorClass(FlightComparator.class);
        // Used to define how map output keys are sorted
        job.setSortComparatorClass(FlightSortComparator.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
