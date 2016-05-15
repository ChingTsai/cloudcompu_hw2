package cloudCompu.PageRankMr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankMr {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "PageRankMr");
		job.setJarByClass(PageRankMr.class);
		// set input format

		// job.setInputFormatClass(KeyValueTextInputFormat.class);
		/*
		 * Test String : 1,AG 3,BB TextInputFormat: (0,AG) (16,BB)
		 * KeyValueTextInputFormat: (1,AG) (3,BB) Can be config with
		 * mapreduce.input.keyvaluelinerecordreader.key.value.separato
		 */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringArrayWritable.class);
		// set the number of reducer
		job.setNumReduceTasks(50);
		// setthe class of each stage in mapreduce
		job.setMapperClass(ParseMapper.class);
		job.setReducerClass(ParseReduce.class);

		// job.setMapperClass(xxx.class);
		// job.setPartitionerClass(xxx.class);
		// job.setSortComparatorClass(xxx.class);
		// job.setReducerClass(xxx.class);

		// set the output class of Mapper and Reducer

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
