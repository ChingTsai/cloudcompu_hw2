package cloudCompu.PageRankMr;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankMr {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setDouble("alpha", 0.85d);

		Job job1 = Job.getInstance(conf, "PageRankMr-Parse");
		job1.setJarByClass(PageRankMr.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		// set the number of reducer
		job1.setNumReduceTasks(50);
		// setthe class of each stage in mapreduce
		job1.setMapperClass(ParseMapper.class);
		job1.setReducerClass(ParseReduce.class);

		// job.setMapperClass(xxx.class);
		// job.setPartitionerClass(xxx.class);
		// job.setSortComparatorClass(xxx.class);
		// job.setReducerClass(xxx.class);

		// set the output class of Mapper and Reducer

		// add input/output path
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("Hw2/tmp"));
		job1.waitForCompletion(true);
		//Thread.currentThread().wait(100);
		long N = job1
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();
		System.out.println("N:" + N);
		conf.setLong("N", N);
		job1.killJob();
		
		Job job2 = Job.getInstance(conf, "PageRankMr-Parse");
		job2.setJarByClass(PageRankMr.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(50);
		// setthe class of each stage in mapreduce
		job2.setMapperClass(PruneMapper.class);
		job2.setReducerClass(PruneReduce.class);

		FileInputFormat.addInputPath(job2, new Path("Hw2/tmp"));
		FileOutputFormat.setOutputPath(job2, new Path("Hw2/pr"));
		
		job2.waitForCompletion(true);

		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("Hw2/tmp"), true);

		Job job3 = Job.getInstance(conf, "PageRankMr-CompuDangle");
		job3.setJarByClass(PageRankMr.class);
		job3.setInputFormatClass(KeyValueTextInputFormat.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(DoubleWritable.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setNumReduceTasks(1);
		// setthe class of each stage in mapreduce
		job3.setMapperClass(CompuDanglMapper.class);
		job3.setReducerClass(CompuDanglReduce.class);

		FileInputFormat.addInputPath(job3, new Path("Hw2/pr"));
		FileOutputFormat.setOutputPath(job3, new Path("Hw2/tmp"));
		job3.waitForCompletion(true);

		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(new Path("Hw2/tmp/part-r-00000"))));
		
		String dangl;
		StringTokenizer tokens = new StringTokenizer(br.readLine());
		tokens.nextToken();
		dangl = tokens.nextToken();
		System.out.println(dangl);
		conf.setDouble("dangl", Double.parseDouble(dangl));
		
		Job job4 = Job.getInstance(conf, "PageRankMr-CompuNextPr");
		job4.setJarByClass(PageRankMr.class);
		job4.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setNumReduceTasks(50);
		// setthe class of each stage in mapreduce
		job4.setMapperClass(CompuNextPrMapper.class);
		job4.setReducerClass(CompuNextPrReduce.class);

		FileInputFormat.addInputPath(job4, new Path("Hw2/pr"));
		FileOutputFormat.setOutputPath(job4, new Path(args[1]));
		job4.waitForCompletion(true);

		System.exit(1);

	}
}
