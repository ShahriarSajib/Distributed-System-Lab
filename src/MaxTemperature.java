import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {
    public static class TemperatureMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text year = new Text();
        private final IntWritable temp = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            if (parts.length == 2) {
                year.set(parts[0].trim());
                temp.set(Integer.parseInt(parts[1].trim()));
                context.write(year, temp);
            }
        }
    }

    public static class MaxReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text year, Iterable<IntWritable> temps, Context context)
                throws IOException, InterruptedException {

            int max = Integer.MIN_VALUE;

            for (IntWritable temp : temps) {
                if (temp.get() > max) {
                    max = temp.get();
                }
            }

            context.write(year, new IntWritable(max));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Temperature");

        job.setJarByClass(MaxTemperature.class);
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(MaxReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
