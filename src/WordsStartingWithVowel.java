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

public class WordsStartingWithVowel {
    public static class VowelMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private static final Text KEY = new Text("words starting with vowel");

        private boolean startsWithVowel(String w) {
            char c = Character.toLowerCase(w.charAt(0));
            return c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u';
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            for (String w : value.toString()
                    .replaceAll("[^a-zA-Z]+", " ").trim().split("\\s+")) {
                if (startsWithVowel(w)) {
                    context.write(KEY, ONE);
                }
            }
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
            System.err.println("Usage: WordsStartingWithVowel <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Words Starting With Vowel");

        job.setJarByClass(WordsStartingWithVowel.class);
        job.setMapperClass(VowelMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
