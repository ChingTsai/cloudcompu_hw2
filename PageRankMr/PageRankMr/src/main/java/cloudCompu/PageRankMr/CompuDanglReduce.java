package cloudCompu.PageRankMr;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CompuDanglReduce extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private Text title = new Text();
	private DoubleWritable pr = new DoubleWritable();

	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double tmppr = 0.0d;
		double alpha = context.getConfiguration().getDouble("alpha", 0.85);
		long N = context.getConfiguration().getLong("N", 1);
		for (DoubleWritable val : values) {
			tmppr += val.get();
		}
		title.set("Dangle");
		pr.set(tmppr*alpha/N);
		context.write(title, pr);
	}
}
