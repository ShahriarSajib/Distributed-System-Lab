# How to Run (WSL + Hadoop)

This guide has a section for **every program** (40 total). Each section tells you:

- what the program does
- which input file it needs
- the exact commands to run it
- the output file to open
- what you should see

## Setup (run once)

### 1. Start Hadoop

```bash
start-dfs.sh
start-yarn.sh
jps
```

You should see `NameNode`, `DataNode`, `ResourceManager`, `NodeManager`.

### 2. Create the input files on your computer

```bash
cat > input.txt <<EOF
Hadoop powers big data applications.
MapReduce is a powerful tool in distributed computing.
EOF

cat > numbers.txt <<EOF
Order 200 units for $95 each and 3 free
Payment of 500 received
EOF

cat > temp.csv <<EOF
2010,45
2010,38
2011,41
2011,50
2012,29
2012,44
EOF

cat > salary.csv <<EOF
HR,50000
IT,70000
IT,65000
HR,55000
Sales,40000
IT,80000
EOF

cat > emails.txt <<EOF
alice@gmail.com bought 10 items
bob@yahoo.com and carol@gmail.com met
bob@yahoo.com
EOF

cat > logs.txt <<EOF
INFO Server started
ERROR Disk full
WARN High memory
INFO User logged in
ERROR Connection reset
FATAL Crash
EOF

cat > ips.txt <<EOF
192.168.1.1 GET /home
10.0.0.2 POST /login
192.168.1.1 GET /about
172.16.0.5 GET /
EOF

mkdir -p multi
echo "one two" > multi/a.txt
echo "two three" > multi/b.txt
```

### 3. Upload the inputs to HDFS

```bash
hdfs dfs -mkdir -p /lab/input /lab/num /lab/temp /lab/salary /lab/email /lab/log /lab/ip /lab/multi
hdfs dfs -put -f input.txt /lab/input/
hdfs dfs -put -f numbers.txt /lab/num/
hdfs dfs -put -f temp.csv /lab/temp/
hdfs dfs -put -f salary.csv /lab/salary/
hdfs dfs -put -f emails.txt /lab/email/
hdfs dfs -put -f logs.txt /lab/log/
hdfs dfs -put -f ips.txt /lab/ip/
hdfs dfs -put -f multi/a.txt multi/b.txt /lab/multi/
```

### 4. Build the jars

```bash
./build_all.sh
```

Creates `build/jars/<Program>.jar` for every program.

---

# Part A - Text programs (run on /lab/input)

## 1. LongestWord

Finds the longest word(s). Ignores punctuation, keeps original case.

```bash
hdfs dfs -rm -r -f /lab/lw-out
hadoop jar build/jars/LongestWord.jar LongestWord /lab/input /lab/lw-out
hdfs dfs -cat /lab/lw-out/part-r-00000
```

Expected: `applications`

## 2. CaseInsensitiveWordCount

Counts each word, lowercased.

```bash
hdfs dfs -rm -r -f /lab/ciwc-out
hadoop jar build/jars/CaseInsensitiveWordCount.jar CaseInsensitiveWordCount /lab/input /lab/ciwc-out
hdfs dfs -cat /lab/ciwc-out/part-r-00000
```

Expected: `a 1`, `applications 1`, `big 1`, `computing 1`, `data 1`, `distributed 1`, `hadoop 1`, `in 1`, `is 1`, `mapreduce 1`, `powerful 1`, `powers 1`, `tool 1`

## 3. WordLengthCount

Counts how many words have each length.

```bash
hdfs dfs -rm -r -f /lab/wlc-out
hadoop jar build/jars/WordLengthCount.jar WordLengthCount /lab/input /lab/wlc-out
hdfs dfs -cat /lab/wlc-out/part-r-00000
```

Expected:

```
1  1
2  2
3  1
4  2
6  2
8  1
9  2
11 1
12 1
```

## 4. WordCount

Classic word count, case-sensitive.

```bash
hdfs dfs -rm -r -f /lab/wc-out
hadoop jar build/jars/WordCount.jar WordCount /lab/input /lab/wc-out
hdfs dfs -cat /lab/wc-out/part-r-00000
```

Expected: `Hadoop 1`, `MapReduce 1`, `applications 1`, ... one line per word.

## 5. AverageWordLength

Average word length of the whole file.

```bash
hdfs dfs -rm -r -f /lab/awl-out
hadoop jar build/jars/AverageWordLength.jar AverageWordLength /lab/input /lab/awl-out
hdfs dfs -cat /lab/awl-out/part-r-00000
```

Expected: `average word length 5.923076923076923`

## 6. CharacterCount

Frequency of each letter (lowercase).

```bash
hdfs dfs -rm -r -f /lab/cc-out
hadoop jar build/jars/CharacterCount.jar CharacterCount /lab/input /lab/cc-out
hdfs dfs -cat /lab/cc-out/part-r-00000
```

Expected: one line per letter, e.g. `a 7`, `i 8`, `o 8`, ...

## 7. FirstLetterCount

Counts words by their starting letter.

```bash
hdfs dfs -rm -r -f /lab/flc-out
hadoop jar build/jars/FirstLetterCount.jar FirstLetterCount /lab/input /lab/flc-out
hdfs dfs -cat /lab/flc-out/part-r-00000
```

Expected: `a 2`, `b 1`, `c 1`, `d 2`, `h 1`, `i 2`, `m 1`, `p 2`, `t 1`

## 8. VowelConsonantCount

Total vowels vs consonants.

```bash
hdfs dfs -rm -r -f /lab/vc-out
hadoop jar build/jars/VowelConsonantCount.jar VowelConsonantCount /lab/input /lab/vc-out
hdfs dfs -cat /lab/vc-out/part-r-00000
```

Expected:

```
consonants  45
vowels      32
```

## 9. PalindromeFinder

Finds words that read the same backwards.

```bash
hdfs dfs -rm -r -f /lab/pal-out
hadoop jar build/jars/PalindromeFinder.jar PalindromeFinder /lab/input /lab/pal-out
hdfs dfs -cat /lab/pal-out/part-r-00000
```

Expected: `a 1`

## 10. AlphabeticalWordSort

Unique lowercase words sorted a-z with counts.

```bash
hdfs dfs -rm -r -f /lab/aws-out
hadoop jar build/jars/AlphabeticalWordSort.jar AlphabeticalWordSort /lab/input /lab/aws-out
hdfs dfs -cat /lab/aws-out/part-r-00000
```

Expected: `a 1`, `applications 1`, `big 1`, `computing 1`, `data 1`, `distributed 1`, `hadoop 1`, `in 1`, `is 1`, `mapreduce 1`, `powerful 1`, `powers 1`, `tool 1`

## 11. LineCount

Number of lines in the file.

```bash
hdfs dfs -rm -r -f /lab/lc-out
hadoop jar build/jars/LineCount.jar LineCount /lab/input /lab/lc-out
hdfs dfs -cat /lab/lc-out/part-r-00000
```

Expected: `total lines 2`

## 12. DistinctWordsCount

Number of unique words.

```bash
hdfs dfs -rm -r -f /lab/dwc-out
hadoop jar build/jars/DistinctWordsCount.jar DistinctWordsCount /lab/input /lab/dwc-out
hdfs dfs -cat /lab/dwc-out/part-r-00000
```

Expected: `distinct words 13`

## 13. ReverseWord

Every word reversed.

```bash
hdfs dfs -rm -r -f /lab/rev-out
hadoop jar build/jars/ReverseWord.jar ReverseWord /lab/input /lab/rev-out
hdfs dfs -cat /lab/rev-out/part-r-00000
```

Expected: `atad 1`, `detubirtsid 1`, `poodaH 1`, `snoitacilppa 1`, ...

## 14. ShortestWord

Shortest word(s) in the file.

```bash
hdfs dfs -rm -r -f /lab/sw-out
hadoop jar build/jars/ShortestWord.jar ShortestWord /lab/input /lab/sw-out
hdfs dfs -cat /lab/sw-out/part-r-00000
```

Expected: `a`

## 15. TotalWordCount

Total number of words.

```bash
hdfs dfs -rm -r -f /lab/tw-out
hadoop jar build/jars/TotalWordCount.jar TotalWordCount /lab/input /lab/tw-out
hdfs dfs -cat /lab/tw-out/part-r-00000
```

Expected: `total words 13`

## 16. SentenceCount

Number of sentences (split on `.`, `!`, `?`).

```bash
hdfs dfs -rm -r -f /lab/sc-out
hadoop jar build/jars/SentenceCount.jar SentenceCount /lab/input /lab/sc-out
hdfs dfs -cat /lab/sc-out/part-r-00000
```

Expected: `sentences 2`

## 17. AverageWordsPerSentence

Average words per sentence.

```bash
hdfs dfs -rm -r -f /lab/awps-out
hadoop jar build/jars/AverageWordsPerSentence.jar AverageWordsPerSentence /lab/input /lab/awps-out
hdfs dfs -cat /lab/awps-out/part-r-00000
```

Expected: `average words per sentence 6.5`

## 18. LastLetterCount

Counts words by their last letter.

```bash
hdfs dfs -rm -r -f /lab/llc-out
hadoop jar build/jars/LastLetterCount.jar LastLetterCount /lab/input /lab/llc-out
hdfs dfs -cat /lab/llc-out/part-r-00000
```

Expected: `a 2`, `d 1`, `e 1`, `g 2`, `l 2`, `n 1`, `p 1`, `s 3`

## 19. WordsStartingWithVowel

Counts words starting with a vowel.

```bash
hdfs dfs -rm -r -f /lab/wsv-out
hadoop jar build/jars/WordsStartingWithVowel.jar WordsStartingWithVowel /lab/input /lab/wsv-out
hdfs dfs -cat /lab/wsv-out/part-r-00000
```

Expected: `words starting with vowel 4`

## 20. EvenOddWordLengths

Counts even-length vs odd-length words.

```bash
hdfs dfs -rm -r -f /lab/eo-out
hadoop jar build/jars/EvenOddWordLengths.jar EvenOddWordLengths /lab/input /lab/eo-out
hdfs dfs -cat /lab/eo-out/part-r-00000
```

Expected:

```
even length words  8
odd length words   5
```

## 21. CapitalizedWordCount

Counts words starting with a capital letter.

```bash
hdfs dfs -rm -r -f /lab/cap-out
hadoop jar build/jars/CapitalizedWordCount.jar CapitalizedWordCount /lab/input /lab/cap-out
hdfs dfs -cat /lab/cap-out/part-r-00000
```

Expected: `capitalized words 2`

## 22. StopWordFilter

Word count with common stop words removed.

```bash
hdfs dfs -rm -r -f /lab/stop-out
hadoop jar build/jars/StopWordFilter.jar StopWordFilter /lab/input /lab/stop-out
hdfs dfs -cat /lab/stop-out/part-r-00000
```

Expected: `applications 1`, `big 1`, `computing 1`, `data 1`, `distributed 1`, `hadoop 1`, `mapreduce 1`, `powerful 1`, `powers 1`, `tool 1`

## 23. RepeatedWords

Words that appear more than once.

```bash
hdfs dfs -rm -r -f /lab/rep-out
hadoop jar build/jars/RepeatedWords.jar RepeatedWords /lab/input /lab/rep-out
hdfs dfs -cat /lab/rep-out/part-r-00000
```

Expected: (empty - no word repeats in the sample)

## 24. WordsPerLine

Word count for each line.

```bash
hdfs dfs -rm -r -f /lab/wpl-out
hadoop jar build/jars/WordsPerLine.jar WordsPerLine /lab/input /lab/wpl-out
hdfs dfs -cat /lab/wpl-out/part-r-00000
```

Expected: `line 0 5` and `line 37 8` (offsets vary with the file)

## 25. LongestWordPerLine

Longest word of each line.

```bash
hdfs dfs -rm -r -f /lab/lwpl-out
hadoop jar build/jars/LongestWordPerLine.jar LongestWordPerLine /lab/input /lab/lwpl-out
hdfs dfs -cat /lab/lwpl-out/part-r-00000
```

Expected: `applications` and `distributed`

## 26. UpperCaseConverter

Whole file converted to UPPERCASE. Map-only job, so the output is `part-m-00000`.

```bash
hdfs dfs -rm -r -f /lab/uc-out
hadoop jar build/jars/UpperCaseConverter.jar UpperCaseConverter /lab/input /lab/uc-out
hdfs dfs -cat /lab/uc-out/part-m-00000
```

Expected:

```
HADOOP POWERS BIG DATA APPLICATIONS.
MAPREDUCE IS A POWERFUL TOOL IN DISTRIBUTED COMPUTING.
```

## 27. WhitespaceCounter

Counts spaces and punctuation.

```bash
hdfs dfs -rm -r -f /lab/ws-out
hadoop jar build/jars/WhitespaceCounter.jar WhitespaceCounter /lab/input /lab/ws-out
hdfs dfs -cat /lab/ws-out/part-r-00000
```

Expected: `spaces and punctuation 13`

## 28. BigramCount

Counts adjacent word pairs.

```bash
hdfs dfs -rm -r -f /lab/bg-out
hadoop jar build/jars/BigramCount.jar BigramCount /lab/input /lab/bg-out
hdfs dfs -cat /lab/bg-out/part-r-00000
```

Expected: `hadoop powers 1`, `powers big 1`, `big data 1`, `data applications 1`, `mapreduce is 1`, ...

## 29. AnagramFinder

Groups anagrams together (sorted letters as key).

```bash
hdfs dfs -rm -r -f /lab/anag-out
hadoop jar build/jars/AnagramFinder.jar AnagramFinder /lab/input /lab/anag-out
hdfs dfs -cat /lab/anag-out/part-r-00000
```

Expected: key = sorted letters, value = original word, e.g. `aadhoop Hadoop`.

## 30. MostFrequentWord

The single most frequent word.

```bash
hdfs dfs -rm -r -f /lab/mfw-out
hadoop jar build/jars/MostFrequentWord.jar MostFrequentWord /lab/input /lab/mfw-out
hdfs dfs -cat /lab/mfw-out/part-r-00000
```

Expected: `a 1` (all words appear once, so the first one wins)

## 31. SumNumbers

Sums all numbers found in the text. Input: `/lab/num/numbers.txt`.

```bash
hdfs dfs -rm -r -f /lab/sumnum-out
hadoop jar build/jars/SumNumbers.jar SumNumbers /lab/num /lab/sumnum-out
hdfs dfs -cat /lab/sumnum-out/part-r-00000
```

Expected: `sum of numbers 798` (200 + 95 + 3 + 500)

## 32. TopFrequentWords

The 5 most frequent words.

```bash
hdfs dfs -rm -r -f /lab/top-out
hadoop jar build/jars/TopFrequentWords.jar TopFrequentWords /lab/input /lab/top-out
hdfs dfs -cat /lab/top-out/part-r-00000
```

Expected: `a 1`, `applications 1`, `big 1`, `computing 1`, `data 1`

---

# Part B - Structured programs

## 33. MaxTemperature

Max temperature per year. Input: `/lab/temp/temp.csv` (`year,temp` per line).

```bash
hdfs dfs -rm -r -f /lab/max-out
hadoop jar build/jars/MaxTemperature.jar MaxTemperature /lab/temp /lab/max-out
hdfs dfs -cat /lab/max-out/part-r-00000
```

Expected:

```
2010  45
2011  50
2012  44
```

## 34. AverageTempPerYear

Average temperature per year. Input: `/lab/temp/temp.csv`.

```bash
hdfs dfs -rm -r -f /lab/avgtemp-out
hadoop jar build/jars/AverageTempPerYear.jar AverageTempPerYear /lab/temp /lab/avgtemp-out
hdfs dfs -cat /lab/avgtemp-out/part-r-00000
```

Expected:

```
2010  41.5
2011  45.5
2012  36.5
```

## 35. AverageSalaryPerDept

Average salary per department. Input: `/lab/salary/salary.csv` (`dept,salary` per line).

```bash
hdfs dfs -rm -r -f /lab/avgsal-out
hadoop jar build/jars/AverageSalaryPerDept.jar AverageSalaryPerDept /lab/salary /lab/avgsal-out
hdfs dfs -cat /lab/avgsal-out/part-r-00000
```

Expected:

```
HR      52500.0
IT      71666.66666666667
Sales   40000.0
```

## 36. MaxSalaryPerDept

Max salary per department. Input: `/lab/salary/salary.csv`.

```bash
hdfs dfs -rm -r -f /lab/maxsal-out
hadoop jar build/jars/MaxSalaryPerDept.jar MaxSalaryPerDept /lab/salary /lab/maxsal-out
hdfs dfs -cat /lab/maxsal-out/part-r-00000
```

Expected:

```
HR      55000
IT      80000
Sales   40000
```

## 37. EmailDomainCount

Counts emails per domain. Input: `/lab/email/emails.txt`.

```bash
hdfs dfs -rm -r -f /lab/email-out
hadoop jar build/jars/EmailDomainCount.jar EmailDomainCount /lab/email /lab/email-out
hdfs dfs -cat /lab/email-out/part-r-00000
```

Expected:

```
gmail.com  2
yahoo.com  2
```

## 38. LogLevelCounter

Counts log lines per level. Input: `/lab/log/logs.txt` (level is the first word).

```bash
hdfs dfs -rm -r -f /lab/log-out
hadoop jar build/jars/LogLevelCounter.jar LogLevelCounter /lab/log /lab/log-out
hdfs dfs -cat /lab/log-out/part-r-00000
```

Expected:

```
ERROR  2
FATAL  1
INFO   2
WARN   1
```

## 39. IPAddressCount

Counts occurrences of each IP. Input: `/lab/ip/ips.txt`.

```bash
hdfs dfs -rm -r -f /lab/ip-out
hadoop jar build/jars/IPAddressCount.jar IPAddressCount /lab/ip /lab/ip-out
hdfs dfs -cat /lab/ip-out/part-r-00000
```

Expected:

```
10.0.0.2      1
172.16.0.5    1
192.168.1.1   2
```

## 40. WordCountPerFile

Word frequency per input file. Input: `/lab/multi/` (two files).

```bash
hdfs dfs -rm -r -f /lab/perfile-out
hadoop jar build/jars/WordCountPerFile.jar WordCountPerFile /lab/multi /lab/perfile-out
hdfs dfs -cat /lab/perfile-out/part-r-00000
```

Expected:

```
a.txt   one    1
a.txt   two    1
b.txt   three  1
b.txt   two    1
```

---

# If you change an input file

Re-upload it with `-f`, delete the old output, and run the program again:

```bash
hdfs dfs -put -f input.txt /lab/input/
hdfs dfs -rm -r -f /lab/lw-out
hadoop jar build/jars/LongestWord.jar LongestWord /lab/input /lab/lw-out
hdfs dfs -cat /lab/lw-out/part-r-00000
```

# Common errors

- `put: File exists` -> add `-f` when re-uploading.
- `Output directory already exists` -> delete it first with `hdfs dfs -rm -r -f`.
- `NativeCodeLoader` warning -> harmless, ignore it.
- If a program's result looks mixed, your input folder contains files of another format. Keep one format per folder.
