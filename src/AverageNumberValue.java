import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class AverageNumberValue {
    private static final Pattern NUMBER = Pattern.compile("\\d+");

    public static class NumberMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final Text KEY = new Text("average number value");

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

    public static class AverageReducer
            extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            long total = 0;
            long count = 0;

            for (IntWritable value : values) {
                total += value.get();
                count++;
            }

            context.write(key, new DoubleWritable((double) total / count));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageNumberValue <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Number Value");

        job.setJarByClass(AverageNumberValue.class);
        job.setMapperClass(NumberMapper.class);
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
