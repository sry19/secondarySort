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
import java.util.ArrayList;
import java.util.Objects;

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
            if (Objects.equals(res[2], "") || Objects.equals(res[7], "") || Objects.equals(res[37], "")) {
                return;
            }
            if (!res[41].equals("0.00") || !res[43].equals("0.00")) {
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
            return (flightKey.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }



    public static class SecondaryReducer extends Reducer<FlightKey, Text, Text, Text> {
        public void reduce(FlightKey flightKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // System.out.println(flightKey.toString());
            String flightName = flightKey.getAirlineName().toString();
            String s = "";
            ArrayList<String> delays = new ArrayList<>();
            double delaySum = 0.0;
            int delayCount = 0;
            int prevMonth = 1;
            for (Text value: values) {
                int m = Integer.parseInt(flightKey.getMonth().toString());
                if (m == prevMonth) {
                    delayCount += 1;
                    delaySum += Double.parseDouble(value.toString());
                } else if (delayCount != 0){
                    int ave = (int) Math.ceil(delaySum / delayCount);
                    delays.add("("+prevMonth+","+ave+")");
                    delayCount = 0;
                    delaySum = 0.0;
                    prevMonth = m;
                }
            }
            if (delayCount != 0) {
                int ave = (int) Math.ceil(delaySum / delayCount);
                delays.add("("+(prevMonth)+","+ave+")");
            }
            context.write(new Text(flightKey.getAirlineName()), new Text(delays.toString()));
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
