//package com.usc.webber;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.logging.Level;
import java.util.logging.Logger;


class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

    //    private final static IntWritable one = new IntWritable(1);
    private final static Logger LOGGER = Logger.getLogger(WordCountMapper.class.getName());
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] lines = value.toString().split("\\t", 2);
        String documentID = lines[0];
        String line = lines[1].replaceAll("[^a-zA-Z]", " ").toLowerCase();
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            String nextToken = tokenizer.nextToken();
            word.set(nextToken);
            context.write(word, new Text(documentID));
        }
    }
}

class WordCountReducer extends Reducer<Text, Text, Text, Text> {

    private HashMap<String, Integer> invertedIndex = new HashMap<>();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {

            String documentID = value.toString();
            int frequency = 1;

            // Get the occurrence of words
            if (invertedIndex.containsKey(documentID)) {
                frequency = invertedIndex.get(documentID) + 1;
            }

            invertedIndex.put(documentID, frequency);
        }

        // Function to convert HashMap to string
        String invertedIndexString = "";

        for (Map.Entry<String, Integer> entry : invertedIndex.entrySet()) {
            String documentID = entry.getKey();
            Integer frequency = entry.getValue();

            invertedIndexString = invertedIndexString + " " + documentID + ":" + frequency;
        }

        context.write(key, new Text(invertedIndexString));
    }
}

public class WordCount {

    private final static Logger LOGGER = Logger.getLogger(WordCountMapper.class.getName());

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 2) {
            System.err.println("Usage: Word Count <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(WordCount.class);
        job.setJobName("Word Count");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
}
