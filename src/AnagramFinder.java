import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnagramFinder {
    private static String sortedKey(String word) {
        char[] chars = word.toLowerCase().toCharArray();
        Arrays.sort(chars);
        return new String(chars);
    }

    public static class AnagramMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private final Text keyOut = new Text();
        private final Text wordOut = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            for (String w : value.toString()
                    .replaceAll("[^a-zA-Z]+", " ").trim().split("\\s+")) {
                keyOut.set(sortedKey(w));
                wordOut.set(w);
                context.write(keyOut, wordOut);
            }
        }
    }

    public static class AnagramReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text word : values) {
                context.write(key, word);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AnagramFinder <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Anagram Finder");

        job.setJarByClass(AnagramFinder.class);
        job.setMapperClass(AnagramMapper.class);
        job.setReducerClass(AnagramReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
