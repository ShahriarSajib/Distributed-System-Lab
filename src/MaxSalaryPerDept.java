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

public class MaxSalaryPerDept {
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

    public static class MaxReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text dept, Iterable<IntWritable> salaries, Context context)
                throws IOException, InterruptedException {

            int max = Integer.MIN_VALUE;

            for (IntWritable salary : salaries) {
                if (salary.get() > max) {
                    max = salary.get();
                }
            }

            context.write(dept, new IntWritable(max));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxSalaryPerDept <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Salary Per Dept");

        job.setJarByClass(MaxSalaryPerDept.class);
        job.setMapperClass(SalaryMapper.class);
        job.setReducerClass(MaxReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
