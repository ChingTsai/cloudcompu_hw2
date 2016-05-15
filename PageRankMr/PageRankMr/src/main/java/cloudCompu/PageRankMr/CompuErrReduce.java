package cloudCompu.PageRankMr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CompuErrReduce extends Reducer<Text, Text, Text, Text> {
	private Text title = new Text();
	private Text err = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double tmppr = 0.0d;
		/*
		 * for (DoubleWritable val : values) { tmppr += val.get(); }
		 */
		for (Text t : values) {
			title.set(key);
			err.set(t);
			context.write(title, err);

		}
		/*
		 * title.set(key); err.set(String.valueOf(tmppr)); context.write(title,
		 * err);
		 */
	}

}
