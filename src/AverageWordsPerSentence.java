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

public class AverageWordsPerSentence {
    public static class SentenceMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final Text KEY = new Text("avg");
        private final IntWritable count = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            for (String s : value.toString().trim().split("[.!?]")) {
                String[] words = s.replaceAll("[^a-zA-Z]+", " ").trim().split("\\s+");

                if (words.length > 0 && !words[0].isEmpty()) {
                    count.set(words.length);
                    context.write(KEY, count);
                }
            }
        }
    }

    public static class AverageReducer
            extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            long total = 0;
            long sentences = 0;

            for (IntWritable value : values) {
                total += value.get();
                sentences++;
            }

            context.write(new Text("average words per sentence"),
                    new DoubleWritable((double) total / sentences));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageWordsPerSentence <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Words Per Sentence");

        job.setJarByClass(AverageWordsPerSentence.class);
        job.setMapperClass(SentenceMapper.class);
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
