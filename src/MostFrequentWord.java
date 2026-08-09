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

public class MostFrequentWord {
    public static class WordMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            for (String w : value.toString()
                    .replaceAll("[^a-zA-Z]+", " ").trim().split("\\s+")) {
                word.set(w.toLowerCase());
                context.write(word, ONE);
            }
        }
    }

    public static class FrequencyReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private String topWord = "";
        private int topCount = 0;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            if (sum > topCount) {
                topCount = sum;
                topWord = key.toString();
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            context.write(new Text(topWord), new IntWritable(topCount));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MostFrequentWord <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most Frequent Word");

        job.setJarByClass(MostFrequentWord.class);
        job.setMapperClass(WordMapper.class);
        job.setCombinerClass(FrequencyReducer.class);
        job.setReducerClass(FrequencyReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
