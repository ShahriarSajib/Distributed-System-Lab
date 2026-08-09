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

public class AverageSalaryPerDept {
    public static class SalaryMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text dept = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            if (parts.length == 2) {
                dept.set(parts[0].trim());
                context.write(dept, new IntWritable(Integer.parseInt(parts[1].trim())));
            }
        }
    }

    public static class AverageReducer
            extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text dept, Iterable<IntWritable> salaries, Context context)
                throws IOException, InterruptedException {

            long total = 0;
            long count = 0;

            for (IntWritable salary : salaries) {
                total += salary.get();
                count++;
            }

            context.write(dept, new DoubleWritable((double) total / count));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageSalaryPerDept <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Salary Per Dept");

        job.setJarByClass(AverageSalaryPerDept.class);
        job.setMapperClass(SalaryMapper.class);
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
