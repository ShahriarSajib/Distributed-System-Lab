import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageTempPerYear {
    public static class TempMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text year = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            if (parts.length == 2) {
                year.set(parts[0].trim());
                context.write(year, new IntWritable(Integer.parseInt(parts[1].trim())));
            }
        }
    }

    public static class AverageReducer
            extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text year, Iterable<IntWritable> temps, Context context)
                throws IOException, InterruptedException {

            long total = 0;
            long count = 0;

            for (IntWritable temp : temps) {
                total += temp.get();
                count++;
            }

            context.write(year, new DoubleWritable((double) total / count));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageTempPerYear <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Temp Per Year");

        job.setJarByClass(AverageTempPerYear.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(AverageReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
