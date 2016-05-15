package cloudCompu.PageRankMr;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CompuNextPrReduce extends
		Reducer<Text, StringArrayWritable, Text, Text> {
	private Text title = new Text();
	private Text link = new Text();

	public void reduce(Text key, Iterable<StringArrayWritable> values,
			Context context) throws IOException, InterruptedException {
		Long N = context.getConfiguration().getLong("N", 1);
		StringBuilder sb = new StringBuilder("");

		double pr = 0.0d;
		double prepr = 0.0d;
		double outpr = 0.0d;
		for (StringArrayWritable val : values) {
			if (val.getLen() != 0) {
				for (Text t : (Text[]) val.toArray()) {
					sb.append(" ");
					sb.append(t);
				}
				pr = val.getPr();
				prepr = val.getprePr();
			} else {
				outpr += val.getPr();
			}
		}
		sb.insert(0, String.valueOf(pr + outpr / N));
		sb.insert(0, " ");
		sb.insert(0, String.valueOf(prepr));
		title.set(key);
		link.set(sb.toString());
		context.write(title, link);
	}
}
