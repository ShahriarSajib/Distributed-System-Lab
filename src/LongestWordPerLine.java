import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LongestWordPerLine {
    public static class LineMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private final Text lineId = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String longest = "";

            for (String w : value.toString()
                    .replaceAll("[^a-zA-Z]+", " ").trim().split("\\s+")) {
                if (w.length() > longest.length()) {
                    longest = w;
                }
            }

            lineId.set("line " + key.get());
            context.write(lineId, new Text(longest));
        }
    }

    public static class LongestReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String longest = "";

            for (Text value : values) {
                if (value.toString().length() > longest.length()) {
                    longest = value.toString();
                }
            }

            context.write(key, new Text(longest));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LongestWordPerLine <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Longest Word Per Line");

        job.setJarByClass(LongestWordPerLine.class);
        job.setMapperClass(LineMapper.class);
        job.setReducerClass(LongestReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
