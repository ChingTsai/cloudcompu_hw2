package cloudCompu.PageRankMr;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CompuNextPrReduce extends Reducer<Text, Text, Text, Text> {
	private Text title = new Text();
	private Text links = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Long N = context.getConfiguration().getLong("N", 1);
		StringBuilder sb = new StringBuilder("");

		double pr = 0.0d;
		String prepr = null;
		double outpr = 0.0d;
		String[] link;
		int len;
		for (Text val : values) {
			link = val.toString().split(" ");
			len = Integer.parseInt(link[2]);
			if (len != -1) {
				for (int i = 0; i < len; i++) {
					sb.append(" ");
					sb.append(link[i + 2]);
				}
				pr = Double.parseDouble(link[1]);
				prepr = link[0];
			} else {
				outpr += Double.parseDouble(link[0]);
			}
		}
		sb.insert(0, String.valueOf(pr + outpr / N));
		sb.insert(0, " ");
		sb.insert(0, prepr);
		title.set(key);
		links.set(sb.toString());
		context.write(title, links);
	}
}
