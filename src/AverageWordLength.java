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

public class AverageWordLength {
    public static class LengthMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final Text KEY = new Text("avg");
        private final IntWritable length = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            for (String word : value.toString()
                    .replaceAll("[^a-zA-Z]+", " ").trim().split("\\s+")) {
                length.set(word.length());
                context.write(KEY, length);
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

            context.write(new Text("average word length"),
                    new DoubleWritable((double) total / count));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageWordLength <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Word Length");

        job.setJarByClass(AverageWordLength.class);
        job.setMapperClass(LengthMapper.class);
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
