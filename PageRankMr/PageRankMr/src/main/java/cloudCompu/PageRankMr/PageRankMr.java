package cloudCompu.PageRankMr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankMr {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

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
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		job2.waitForCompletion(true);
		long N = job2
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"REDUCE_OUTPUT_RECORDS").getValue();
		System.out.println("NNNNNN :" + N);
		System.exit(1);


	}
}
