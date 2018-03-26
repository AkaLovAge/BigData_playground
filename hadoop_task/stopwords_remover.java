// This file uses Hadoop map-reduce to remove the stop words

package com.example;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class stopwords_remover {
	
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, NullWritable>{
    
    private Text word = new Text();
    private final NullWritable out = NullWritable.get();
    //private final static IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    		String[] stopwords = context.getConfiguration().getStrings("stopwords");
    		StringTokenizer itr = new StringTokenizer(value.toString());
    		while (itr.hasMoreTokens()) {
    			String w = itr.nextToken().toLowerCase();
    			word.set(w);
    			if (! ArrayUtils.contains(stopwords, w)){
    				context.write(word,out);
    			}
    		}
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,NullWritable,Text,NullWritable> {
	private final NullWritable out = NullWritable.get();
    public void reduce(Text key, NullWritable values, 
                       Context context
                       ) throws IOException, InterruptedException {
      context.write(key,out);
    }
  }

  public static void main(String[] args) throws Exception {
	  final String[] stopwords_list = {"a", "about", "above", "after", "again", "against", 
			  "all", "am", "an", "and", "any", "are", "as", "at", "be", "because", "been", "before", 
			  "being", "below", "between", "both", "but", "by", "could", "did", "do", "does", "doing", 
			  "down", "during", "each", "few", "for", "from", "further", "had", "has", "have", "having", 
			  "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", 
			  "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "it", 
			  "it's", "its", "itself", "let's", "me", "more", "most", "my", "myself", "nor", "of", "on", 
			  "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", 
			  "same", "she", "she'd", "she'll", "she's", "should", "so", "some", "such", "than", "that", 
			  "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", 
			  "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", 
			  "under", "until", "up", "very", "was", "we", "we'd", "we'll", "we're", "we've", "were", "what", 
			  "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", 
			  "why's", "with", "would", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", 
			  "yourselves"};
	  
    Configuration conf = new Configuration();
    
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
  
    conf.setStrings("stopwords",stopwords_list);
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(stopwords_remover.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
        
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
