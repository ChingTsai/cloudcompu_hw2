package cloudCompu.PageRankMr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CompuDanglReduce extends Reducer<Text, DoubleWritable, Text, Text> {
	private Text title = new Text();
	private Text pr = new Text();

	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double tmppr = 0.0d;
		double alpha = context.getConfiguration().getDouble("alpha", 0.85);
		Long N = context.getConfiguration().getLong("N", 1);

		for (DoubleWritable val : values) {
			tmppr += val.get();
		}

		//title.set("Sum");
		pr.set(String.valueOf(tmppr/N*alpha ));
		context.write(key, pr);
	}
}
