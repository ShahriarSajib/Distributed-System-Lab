# Lab Exam Survival Guide

**Goal:** You get ONE new problem with a fresh `.txt` / `.csv` input. Follow this order and you will never panic.

---

## 0. The 5-Step Golden Workflow

Memorize this order. Do it exactly, every time:

```
1. READ the problem twice.
2. CLASSIFY the task (see Step 1).
3. CHOOSE an existing jar, OR copy a template for a new program (Steps 2-3).
4. PUT the input file into HDFS (Step 4).
5. RUN + CHECK output (Step 5).
```

---

## 1. Classify the Problem (30 seconds)

Answer these 3 questions:

| Question | What it tells you |
|---|---|
| **What is the input format?** | `.txt` = one line of words. `.csv` = `key,value` per line (or 2+ columns). |
| **What is the task?** | Count / Max / Min / Average / Sum / Filter / Transform / Per-line / Per-file. |
| **What is the output?** | One number total? One line per key? One line per word? |

### Task → Existing Program cheat sheet

Most exam problems are just these tasks with **different input**:

| Task | Existing program to use |
|---|---|
| Count words | `WordCount`, `CaseInsensitiveWordCount`, `StopWordFilter` |
| Longest / shortest word | `LongestWord`, `ShortestWord` |
| Count by first/last letter | `FirstLetterCount`, `LastLetterCount` |
| Vowels / consonants | `VowelConsonantCount`, `WordsStartingWithVowel`, `WordsWithTwoVowels` |
| Word length stats | `WordLengthCount`, `EvenOddWordLengths`, `AverageWordLength` |
| Max / min per key (csv) | `MaxTemperature` / `MinTemperature` (change nothing, just new data) |
| Average per key (csv) | `AverageTempPerYear`, `AverageSalaryPerDept` |
| Sum per key (csv) | `TotalSalaryPerDept` |
| Count a thing per key (csv) | `EmailDomainCount`, `LogLevelCounter`, `IPAddressCount` |
| Per line / per file counts | `WordsPerLine`, `NumbersPerLine`, `TotalWordsPerFile`, `LinesPerFile` |
| Whole-file transform (uppercase) | `UpperCaseConverter` (map-only) |
| Numbers in text | `SumNumbers`, `MaxNumber`, `AverageNumberValue`, `NumbersPerLine` |

**Rule of thumb:** if the task says "**per department / per year / per domain / per level / per IP / per file**" it is a **group-by-key** problem = the CSV template below. If it says "**count words that...**" it is the **text template** below.

---

## 2. If an Existing Jar Fits — Just Run It

```bash
# 1. upload the new input (change filename + folder name!)
hdfs dfs -put -f input.txt /lab/input/          # or salary.csv to /lab/salary/

# 2. delete old output + run (change <name> everywhere)
hdfs dfs -rm -r -f /lab/<name>-out
hadoop jar build/jars/<Name>.jar <Name> /lab/input /lab/<name>-out

# 3. view result
hdfs dfs -cat /lab/<name>-out/part-r-00000
```

> For a map-only program (like `UpperCaseConverter`) the output file is
> `part-m-00000`, not `part-r-00000`.

---

## 3. If It Is a NEW Problem — Copy a Template (5 min)

New problem = new Mapper/Reducer logic. **Never write from scratch.** Copy a template, change 3 things:

1. **Class name** (file name = class name)
2. **The Mapper line** (how you turn one line into `key,value`)
3. **(maybe) The Reducer logic** (how values combine)

Then build it (Step 4) and run it (Step 5).

### Template A — Text file, one line of words

```java
import java.io.IOException;
import java.util.StringTokenizer;

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

public class MyProgram {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text outKey = new Text();
        private static final IntWritable ONE = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(
                    value.toString().replaceAll("[^a-zA-Z]+", " "));

            while (itr.hasMoreTokens()) {
                String w = itr.nextToken();
                // <<<< CHANGE THIS LINE >>>>
                // e.g. count words ending in 's':
                //   if (!w.toLowerCase().endsWith("s")) continue;
                // e.g. count by first letter:
                //   outKey.set(w.substring(0, 1).toLowerCase());
                // default = whole word:
                outKey.set(w);
                context.write(outKey, ONE);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MyProgram <input> <output>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "My Program");
        job.setJarByClass(MyProgram.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

### Template B — CSV file, `key,value` per line (group-by-key)

The mapper only changes — everything else stays. For **sum / count / max / min** the reducer is the same. For **average** use the version below.

```java
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

public class MyCsv {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text key = new Text();

        public void map(LongWritable lineNum, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            if (parts.length >= 2) {                 // ignore bad lines
                key.set(parts[0].trim());            // group key = column 1
                int num = Integer.parseInt(parts[1].trim());   // value = column 2
                context.write(key, new IntWritable(num));
            }
        }
    }

    // ==== for SUM / COUNT / MAX / MIN ====
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) sum += v.get();   // MAX: Math.max
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MyCsv <input> <output>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "My Csv");
        job.setJarByClass(MyCsv.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

#### For AVERAGE, replace only the reducer with:

```java
    public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            long total = 0, count = 0;
            for (IntWritable v : values) { total += v.get(); count++; }
            context.write(key, new DoubleWritable((double) total / count));
        }
    }
```

> and add this import + change the output classes in `main`:
>
> ```java
> import org.apache.hadoop.io.DoubleWritable;
> job.setMapOutputValueClass(IntWritable.class);
> job.setOutputKeyClass(Text.class);
> job.setOutputValueClass(DoubleWritable.class);
> ```

### Reducer logic cheat sheet (one line to change)

| Task | Change `int sum = 0; ... += v.get();` to |
|---|---|
| Sum | keep as is |
| Count | `sum += 1;` |
| Max | `if (v.get() > max) max = v.get();` (start `max = Integer.MIN_VALUE`) |
| Min | `if (v.get() < min) min = v.get();` (start `min = Integer.MAX_VALUE`) |
| Average | use the DoubleWritable reducer above |

---

## 4. Build the New Program

Only needed if you wrote a new file in `src/`.

```bash
# save the file as  src/MyProgram.java  (name MUST match class name)
./build_all.sh
```

This compiles everything and creates `build/jars/MyProgram.jar`.

---

## 5. The Full Run Sequence (memorize this)

```bash
# A. Hadoop must be running (once per session)
start-dfs.sh && start-yarn.sh && jps

# B. create input dir + upload the new file
hdfs dfs -mkdir -p /lab/input
hdfs dfs -put -f input.txt /lab/input/

# C. run the program (delete old output FIRST)
hdfs dfs -rm -r -f /lab/out
hadoop jar build/jars/MyProgram.jar MyProgram /lab/input /lab/out

# D. check the result
hdfs dfs -cat /lab/out/part-r-00000      # part-m-00000 if map-only
```

---

## 6. Quick Mental Checklist Before Running

- [ ] Hadoop is up (`jps` shows NameNode + DataNode + ResourceManager)
- [ ] Input uploaded with `-f` (avoids "File exists")
- [ ] Old output deleted (`-rm -r -f`)
- [ ] Jar exists in `build/jars/`
- [ ] Input folder contains **only one format** (don't mix .txt and .csv in one folder)
- [ ] For CSV problems: file has no header row, or parts[0]/parts[1] are the right columns

---

## 7. Common Errors → Fixes

| Error | Fix |
|---|---|
| `File exists` | add `-f` to `hdfs dfs -put` |
| `Output directory ... already exists` | `hdfs dfs -rm -r -f <out>` |
| `ClassNotFoundException: MyProgram` | file name must equal class name; rebuild with `./build_all.sh` |
| `Cannot run program "..."` / java home | run `export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))` in the same shell |
| `NativeCodeLoader ... native-hadoop` warning | harmless, ignore |
| Output is empty or mixed-up | input folder has wrong file(s), or CSV columns are swapped in the mapper |
| `NumberFormatException` on csv line | that line isn't `key,value` — the `if (parts.length >= 2)` guard handles it; check for a header row |
| Result looks "mixed" | one folder must hold only one data format — make a new folder per format |

---

## 8. Pre-Exam Warmup (do this now, 5 minutes)

1. Run `start-dfs.sh && start-yarn.sh && jps` — confirm all daemons.
2. Run `./build_all.sh` — confirm all 50 jars build.
3. Run **one** program end-to-end (e.g. `WordCount`) with `hdfs dfs -put -f` → run → `-cat`.
4. Test the CSV template: make a `test.csv`, upload, run `MaxTemperature` on it.
5. Save this file's Steps 4-5 as a shell snippet you can paste.

If the build or a run fails now, you find out in practice — not in the exam.
