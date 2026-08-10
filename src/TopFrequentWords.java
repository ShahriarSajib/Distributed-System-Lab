import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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

public class TopFrequentWords {
    private static int TOP_N = 5;

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

    public static class TopReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final List<String[]> top = new ArrayList<String[]>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            top.add(new String[] { key.toString(), String.valueOf(sum) });
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            top.sort(new Comparator<String[]>() {
                public int compare(String[] a, String[] b) {
                    return Integer.parseInt(b[1]) - Integer.parseInt(a[1]);
                }
            });

            for (int i = 0; i < top.size() && i < TOP_N; i++) {
                context.write(new Text(top.get(i)[0]),
                        new IntWritable(Integer.parseInt(top.get(i)[1])));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: TopFrequentWords <input> <output> [topN]");
            System.exit(2);
        }

        if (args.length == 3) {
            TOP_N = Integer.parseInt(args[2]);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Frequent Words");

        job.setJarByClass(TopFrequentWords.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(TopReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
