package com.usc.webber;

import java.io.IOException;
import java.util.HashMap;
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


class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //    private final static IntWritable one = new IntWritable(1);
    private final static Logger LOGGER = Logger.getLogger(WordCountMapper.class.getName());
    private Text word = new Text();
    private static IntWritable documentId;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        // Strip document id from the string
        if (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            LOGGER.info(token);
            documentId = new IntWritable(Integer.parseInt(token));
        }


        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());

            context.write(word, documentId);
        }

    }
}

class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private HashMap<String, HashMap<Integer, Integer>> invertedIndex = new HashMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // key = word
        // value = documentId
        String word = key.toString();


        for (IntWritable value : values) {

//            // Get the occurrence of words
//            HashMap<Integer, Integer> frequency;
//
//            if (invertedIndex.containsKey(word)) {
//                frequency = invertedIndex.get(word);
//            } else {
//                frequency = new HashMap<>();
//            }
//
//            invertedIndex.put(word, );

            sum += value.get();
        }

        context.write(key, new IntWritable(sum));
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
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);
    }
}
