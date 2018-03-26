/*
 * This is my Hadoop test for count top 100 words in an larger .txt file
 */

package com.example;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
//import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
//import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.hadoop.mapreduce.Counters;

public class WordCount_top100{
	// by using wc -w, the ulysses has 'word_num' words in total
	// hard-coding, needed to be edited after more familair with Java 
	//private final static int word_num = 267902; 
	
	// the mapper of word counter 
	public static class WordCountMapper 
			extends Mapper<Object, Text, Text, IntWritable>{
		private Map<String,Integer> freq = new HashMap<String, Integer>();
		private Text word = new Text();		
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String tmp_word = itr.nextToken();
				// avoid 's 
				if (tmp_word.contains("'s")) {
					String k = tmp_word.split("(?=')")[0].toLowerCase().replace("\t", "");
					if (! k.isEmpty()) {
						int count = freq.containsKey(k) ? freq.get(k) : 0;
						freq.put(k, count + 1);
					}
				}else{
					Pattern pattern = Pattern.compile("[^a-z A-Z]");
					Matcher matcher = pattern.matcher(tmp_word);
					String re = matcher.replaceAll("");
					String k = re.toLowerCase();
					k.replace("\t", "");
					if (! k.isEmpty()) {
						int count = freq.containsKey(k) ? freq.get(k) : 0;
						freq.put(k, count + 1);
					}
				}
			}
		}//~map class
		protected void cleanup(Context context) 
				throws IOException, InterruptedException{
			Iterator<Map.Entry<String,Integer>> iter = freq.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
				word.set(entry.getKey().toString());
				context.write(word, 
						new IntWritable(Integer.parseInt(entry.getValue().toString())));
			}
		}//~cleanup
	}
	

	private static class ValueComparator implements Comparator<Map.Entry<String,Integer>>  
    {  
		@Override
        public int compare(Map.Entry<String,Integer> m, Map.Entry<String,Integer> n)  
        {  
            return m.getValue()-n.getValue();  
        }  
    }  
	
	// reducer of word counter 
	public static class IntSumReducer
			extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		//private Map <String, Integer> map = new HashMap<String, Integer>();
		private PriorityQueue<Map.Entry<String, Integer>> queue = 
				new PriorityQueue<Map.Entry<String, Integer>>(150,new ValueComparator());
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException{
			
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if (sum > 5) {
				Map.Entry<String, Integer> new_entry = new AbstractMap.SimpleEntry(key.toString(), sum);
				queue.add(new_entry);
				if (queue.size()>100) {
					queue.poll();
				}
				//map.put(key.toString(), sum);
			}
		}//~reduce
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Iterator<Map.Entry<String,Integer>> iter = queue.iterator();
			while (iter.hasNext()){
				Map.Entry<String,Integer> tmp = iter.next();
				result.set(tmp.getValue());
				context.write(new Text(tmp.getKey()), result);
			}
		}//~ cleanup
	}
	
	public static class MergeMapper
			extends Mapper<Text, Text, Text, IntWritable>{
		public void map(Text key, Text value, Context context) 
				throws IOException, InterruptedException {
			context.write(key, new IntWritable(Integer.parseInt(value.toString())));
		}
		
	}
	public static class MergeReducer
			extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		private Map <String, Integer> map = new HashMap<String, Integer>();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException{
	
			int val = 0;
			for (IntWritable value : values) {
				val = value.get();
			}
			map.put(key.toString(), val);
		}//~reduce

		protected void cleanup(Context context) throws IOException, InterruptedException {
			List<Map.Entry<String, Integer>> entryList = new ArrayList<Map.Entry<String,Integer>>(map.entrySet());
			Collections.sort(entryList, new ValueComparator());
			Collections.reverse(entryList);
			for (Map.Entry<String, Integer> entry: entryList.subList(0, 100)) {
				result.set(entry.getValue());
				context.write(new Text(entry.getKey()), result);
			}
		}//~ cleanup
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf_count = new Configuration();
		Configuration conf_merge = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf_count, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		
		// 2 mappers in total 
		conf_count.set("mapred.max.split.size", "133951");
		
		//count the word and reduce 
		Job job_count = Job.getInstance(conf_count, "word count");
 		job_count.setJarByClass(WordCount_top100.class);
		job_count.setMapperClass(WordCountMapper.class);
		job_count.setReducerClass(IntSumReducer.class);
		job_count.setOutputKeyClass(Text.class);
		job_count.setOutputValueClass(IntWritable.class);
		job_count.setOutputFormatClass(TextOutputFormat.class);
		job_count.setNumReduceTasks(2);
		for (int i = 0; i < otherArgs.length - 1; i++) {
			FileInputFormat.addInputPath(job_count, new Path(otherArgs[i]));
		}
		Path tmp_output = new Path("word_count");
		FileOutputFormat.setOutputPath(job_count,tmp_output);
		//ControlledJob cjob_count = new ControlledJob(conf_count);
		//cjob_count.setJob(job_count);
		job_count.waitForCompletion(true);
		
		// merge k reducers' result 
		Job job_merge = Job.getInstance(conf_merge, "word count");
	    job_merge.setInputFormatClass(KeyValueTextInputFormat.class);
		job_merge.setJarByClass(WordCount_top100.class);
		job_merge.setMapperClass(MergeMapper.class);
		job_merge.setMapOutputKeyClass(Text.class);
		job_merge.setReducerClass(MergeReducer.class);
		job_merge.setOutputKeyClass(Text.class);
		job_merge.setOutputValueClass(IntWritable.class);
		job_merge.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job_merge, new Path("word_count/part*"));
		
		FileOutputFormat.setOutputPath(job_merge,new Path(otherArgs[otherArgs.length - 1]));
		//ControlledJob cjob_merge = new ControlledJob(conf_merge);
		//cjob_merge.setJob(job_merge);
		System.exit(job_merge.waitForCompletion(true)? 0 : 1);
		
		//trying to use job controller but fail to use thread to kill the process  
		//set up a job controller
		/*JobControl jobctrl = new JobControl("jobctrl");
		jobctrl.addJob(cjob_count);
		jobctrl.addJob(cjob_merge);
		cjob_merge.addDependingJob(cjob_count);
		jobctrl.run();
		Thread t = new Thread (jobctrl);		
		t.start();
		while(!jobctrl.allFinished()) {
			System.out.println("Sting running");
		}		
		System.exit(0);*/
		//System.exit(job_count.waitForCompletion(true)? 0 : 1);
	}
}


