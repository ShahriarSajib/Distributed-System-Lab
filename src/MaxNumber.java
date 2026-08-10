import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class MaxNumber {
    private static final Pattern NUMBER = Pattern.compile("\\d+");

    public static class NumberMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final Text KEY = new Text("max number");

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Matcher matcher = NUMBER.matcher(value.toString());

            while (matcher.find()) {
                context.write(KEY,
                        new IntWritable(Integer.parseInt(matcher.group())));
            }
        }
    }

    public static class MaxReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int max = Integer.MIN_VALUE;

            for (IntWritable value : values) {
                if (value.get() > max) {
                    max = value.get();
                }
            }

            context.write(key, new IntWritable(max));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxNumber <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Number");

        job.setJarByClass(MaxNumber.class);
        job.setMapperClass(NumberMapper.class);
        job.setCombinerClass(MaxReducer.class);
        job.setReducerClass(MaxReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
