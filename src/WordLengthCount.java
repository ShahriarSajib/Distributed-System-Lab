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

public class WordLengthCount {
    public static class LengthMapper
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final IntWritable length = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String cleaned = value.toString()
                    .replaceAll("[^\\p{L}\\p{Nd}]+", " ")
                    .trim();

            if (cleaned.isEmpty()) {
                return;
            }

            for (String word : cleaned.split("\\s+")) {
                length.set(word.length());
                context.write(length, ONE);
            }
        }
    }

    public static class SumReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(IntWritable length, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            result.set(sum);
            context.write(length, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordLengthCount <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Length Count");

        job.setJarByClass(WordLengthCount.class);
        job.setMapperClass(LengthMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
