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
		Double tmppr = 0.0d;
		Double alpha = context.getConfiguration().getDouble("alpha", 0.85);
		for (DoubleWritable val : values) {
			tmppr += val.get();
		}
		title.set("Dangle");
		pr.set(tmppr*alpha);
		context.write(title, pr);
	}
}
