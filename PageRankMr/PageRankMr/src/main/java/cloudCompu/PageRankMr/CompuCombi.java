package cloudCompu.PageRankMr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CompuCombi extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private Text title = new Text();
	private DoubleWritable tmp = new DoubleWritable();

	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double tmppr = 0.0d;

		for (DoubleWritable val : values) {
			tmppr += val.get();
		}

		title.set(key);
		tmp.set(tmppr);
		context.write(title, tmp);
	}

}
