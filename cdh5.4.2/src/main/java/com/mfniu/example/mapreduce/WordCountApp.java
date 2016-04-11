package com.mfniu.example.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * hadoop历史版本下载：http://apache.fayea.com/hadoop/common/
 * 
 */
public class WordCountApp {

	/**
	 * 
	 * #把测试数据文件传到hdfs
	 * hadoop fs -copyFromLocal word.txt /user/wangya/example/wordcount/word.txt
	 * 
	 * #之后将patent.jar加入环境变量
	 * export HADOOP_CLASSPATH=/mfniu/example/WordCountApp.jar
	 * 
	 * #之后运行这个mapreduce任务
	 * hadoop fs -rm -r -f /user/wangya/example/wordcount/out
	 * hadoop com.mfniu.example.WordCountApp /user/wangya/example/wordcount/word.txt /user/wangya/example/wordcount/out
	 * 
	 */
	public static void main(String[] args) throws Exception {

		System.setProperty("HADOOP_USER_NAME", "root");
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "WordCountApp_Job");

		job.setJarByClass(WordCountApp.class);
		job.setMapperClass(WordCountMapper.class);		
		//job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setNumReduceTasks(1);

		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		Path in = new Path(args[0]);

		Path out = new Path(args[1]);

		FileInputFormat.setInputPaths(job, in);

		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);

	}

}
