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

public class WhitespaceCounter {
    public static class SpaceMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final Text KEY = new Text("spaces and punctuation");

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            int count = 0;

            for (char c : value.toString().toCharArray()) {
                if (!Character.isLetter(c) && !Character.isDigit(c)) {
                    count++;
                }
            }

            context.write(KEY, new IntWritable(count));
        }
    }

    public static class SumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WhitespaceCounter <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Whitespace Counter");

        job.setJarByClass(WhitespaceCounter.class);
        job.setMapperClass(SpaceMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
