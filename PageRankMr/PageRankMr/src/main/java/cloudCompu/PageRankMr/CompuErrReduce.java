package cloudCompu.PageRankMr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CompuErrReduce extends Reducer<Text, DoubleWritable, Text, Text> {
	private Text title = new Text();
	private Text err = new Text();

	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double tmppr = 0.0d;

		for (DoubleWritable val : values) {
			tmppr += val.get();
		}

		title.set(key);
		err.set(String.valueOf(tmppr));
		context.write(title, err);

	}

}
