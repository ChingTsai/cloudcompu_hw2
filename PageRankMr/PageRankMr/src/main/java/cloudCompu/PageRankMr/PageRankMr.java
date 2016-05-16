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

		Path tmp_path = new Path("Hw2/tmp");
		Path err_path = new Path("Hw2/err");
		Path pr_path = new Path("Hw2/pr");
		long st, ed;
		double micros;
		Configuration conf = new Configuration();
		conf.setDouble("alpha", 0.85d);

		st = System.nanoTime();
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
		FileOutputFormat.setOutputPath(job1, tmp_path);
		job1.waitForCompletion(true);
		// Thread.currentThread().wait(100);
		long N = job1
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();
		System.out.println("N:" + N);
		conf.setLong("N", N);

		Job job2 = Job.getInstance(conf, "PageRankMr-Prune");
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

		FileInputFormat.addInputPath(job2, tmp_path);
		FileOutputFormat.setOutputPath(job2, pr_path);

		job2.waitForCompletion(true);

		FileSystem fs = FileSystem.get(conf);
		ed = System.nanoTime();
		micros = (ed - st) / 1000000000d;
		System.out.println("Parse :  " + String.valueOf(micros) + " seconds");
		// loop
		Job job3, job4, job5;
		double Err = 1.0;
		int iter = 0;
		while (Err > 0.1) {
			st = System.nanoTime();
			fs.delete(tmp_path, true);

			job3 = Job.getInstance(conf, "PageRankMr-CompuDangle");
			job3.setJarByClass(PageRankMr.class);
			job3.setInputFormatClass(KeyValueTextInputFormat.class);
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(DoubleWritable.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			job3.setCombinerClass(CompuCombi.class);
			job3.setNumReduceTasks(1);
			// setthe class of each stage in mapreduce
			job3.setMapperClass(CompuDanglMapper.class);
			job3.setReducerClass(CompuDanglReduce.class);

			FileInputFormat.addInputPath(job3, pr_path);
			FileOutputFormat.setOutputPath(job3, tmp_path);
			job3.waitForCompletion(true);

			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(new Path("Hw2/tmp/part-r-00000"))));

			String dangl;
			StringTokenizer tokens = new StringTokenizer(br.readLine());
			tokens.nextToken();
			dangl = tokens.nextToken();

			conf.setDouble("dangl", Double.parseDouble(dangl));

			fs.delete(tmp_path, true);

			job4 = Job.getInstance(conf, "PageRankMr-CompuNextPr");
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

			FileInputFormat.addInputPath(job4, pr_path);
			FileOutputFormat.setOutputPath(job4, tmp_path);
			job4.waitForCompletion(true);

			fs.delete(pr_path, true);

			job5 = Job.getInstance(conf, "PageRankMr-CompuErr");
			job5.setJarByClass(PageRankMr.class);
			job5.setInputFormatClass(KeyValueTextInputFormat.class);
			job5.setMapOutputKeyClass(Text.class);
			job5.setMapOutputValueClass(DoubleWritable.class);
			job5.setOutputKeyClass(Text.class);
			job5.setOutputValueClass(Text.class);
			job5.setNumReduceTasks(1);
			// setthe class of each stage in mapreduce
			job5.setCombinerClass(CompuCombi.class);
			job5.setMapperClass(CompuErrMapper.class);
			job5.setReducerClass(CompuErrReduce.class);

			FileInputFormat.addInputPath(job5, tmp_path);
			FileOutputFormat.setOutputPath(job5, err_path);
			job5.waitForCompletion(true);

			br = new BufferedReader(new InputStreamReader(fs.open(new Path(
					"Hw2/err/part-r-00000"))));
			String err;
			tokens = new StringTokenizer(br.readLine());
			tokens.nextToken();
			err = tokens.nextToken();

			fs.delete(err_path, true);
			fs.delete(pr_path, true);
			fs.rename(tmp_path, pr_path);
			ed = System.nanoTime();
			micros = (ed - st) / 1000000000d;
			Err = Double.parseDouble(err);
			System.out.println("Iteration : " + iter + " err: " + err);
			System.out.println("Compute :  " + String.valueOf(micros)
					+ " seconds");
			iter++;
		}

		Job job6 = Job.getInstance(conf, "PageRankMr-Sort");
		job6.setJarByClass(PageRankMr.class);
		job6.setInputFormatClass(KeyValueTextInputFormat.class);
		job6.setMapOutputKeyClass(Text.class);
		job6.setMapOutputValueClass(Text.class);
		job6.setOutputKeyClass(Text.class);
		job6.setOutputValueClass(Text.class);
		// set the number of reducer
		job6.setNumReduceTasks(1);
		// setthe class of each stage in mapreduce
		job6.setMapperClass(SortMapper.class);
		job6.setReducerClass(SortReduce.class);
		job6.setSortComparatorClass(CompuNextPrCompare.class);
		FileInputFormat.addInputPath(job6, pr_path);
		FileOutputFormat.setOutputPath(job6, new Path(args[1]));
		job6.waitForCompletion(true);

		System.exit(1);

	}
}
