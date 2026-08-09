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

public class VowelConsonantCount {
    public static class VC {
        public static boolean isVowel(char c) {
            c = Character.toLowerCase(c);
            return c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u';
        }
    }

    public static class VCMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private static final Text VOWELS = new Text("vowels");
        private static final Text CONSONANTS = new Text("consonants");

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            for (char c : value.toString().toLowerCase().toCharArray()) {
                if (Character.isLetter(c)) {
                    context.write(VC.isVowel(c) ? VOWELS : CONSONANTS, ONE);
                }
            }
        }
    }

    public static class SumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: VowelConsonantCount <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Vowel Consonant Count");

        job.setJarByClass(VowelConsonantCount.class);
        job.setMapperClass(VCMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
