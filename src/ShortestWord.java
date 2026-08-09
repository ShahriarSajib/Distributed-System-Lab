import java.io.IOException;
import java.util.ArrayList;

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

public class ShortestWord {
    public static class LengthMapper
            extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final IntWritable length = new IntWritable();
        private final Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            for (String w : value.toString()
                    .replaceAll("[^a-zA-Z]+", " ").trim().split("\\s+")) {
                length.set(w.length());
                word.set(w);
                context.write(length, word);
            }
        }
    }

    public static class ShortestReducer
            extends Reducer<IntWritable, Text, NullWritable, Text> {

        private int min = Integer.MAX_VALUE;
        private final ArrayList<String> words = new ArrayList<String>();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) {
            int len = key.get();

            if (len < min) {
                min = len;
                words.clear();

                for (Text value : values) {
                    words.add(value.toString());
                }
            } else if (len == min) {
                for (Text value : values) {
                    words.add(value.toString());
                }
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (String word : words) {
                context.write(NullWritable.get(), new Text(word));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ShortestWord <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Shortest Word(s)");

        job.setJarByClass(ShortestWord.class);
        job.setMapperClass(LengthMapper.class);
        job.setReducerClass(ShortestReducer.class);

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
