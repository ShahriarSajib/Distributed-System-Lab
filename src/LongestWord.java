import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LongestWord {
    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final IntWritable length = new IntWritable();
        private final Text wordOut = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Remove punctuation by replacing non-letter/non-digit characters
            // with spaces. Original word case is preserved.
            String cleaned = line.replaceAll("[^\\p{L}\\p{Nd}]+", " ").trim();

            if (cleaned.isEmpty()) {
                return;
            }

            for (String word : cleaned.split("\\s+")) {
                length.set(word.length());
                wordOut.set(word);
                context.write(length, wordOut);
            }
        }
    }

    public static class LongestReducer
            extends Reducer<IntWritable, Text, NullWritable, Text> {

        private int maxLength = 0;
        private final Set<String> longestWords = new LinkedHashSet<>();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) {
            int len = key.get();

            if (len > maxLength) {
                maxLength = len;
                longestWords.clear();

                for (Text value : values) {
                    longestWords.add(value.toString());
                }
            } else if (len == maxLength) {
                for (Text value : values) {
                    longestWords.add(value.toString());
                }
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (String word : longestWords) {
                context.write(NullWritable.get(), new Text(word));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LongestWord <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Longest Word(s)");

        job.setJarByClass(LongestWord.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(LongestReducer.class);

        // One reducer is required because the maximum must be global.
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
