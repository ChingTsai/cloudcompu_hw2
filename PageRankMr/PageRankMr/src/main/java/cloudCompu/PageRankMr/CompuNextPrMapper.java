package cloudCompu.PageRankMr;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompuNextPrMapper extends Mapper<Text, Text, Text, Text> {
	private Text title = new Text();
	private Text pr = new Text();

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		String[] links = value.toString().split(" ");
		double prepr = Double.parseDouble(links[0]);
		double dangl = context.getConfiguration().getDouble("dangl", 1);
		double alpha = context.getConfiguration().getDouble("alpha", 0.85);
		Long N = context.getConfiguration().getLong("N", 1);
		if (links.length > 2) {
			int c = links.length - 2;
			for (int i = 2; i < links.length; i++) {
				title.set(links[i]);
				pr.set(String.valueOf(prepr / c) + " 0 -1");
				context.write(title, pr);
			}

			title.set(key.toString());
			StringBuilder sb = new StringBuilder("");
			sb.append(links[0]);
			sb.append(" ");
			sb.append(String.valueOf(((1 - alpha) + alpha * dangl) / N));
			for (int i = 2; i < links.length; i++) {
				sb.append(" ");
				sb.append(links[i]);
			}
			sb.append(String.valueOf(links.length - 2));
			pr.set(sb.toString());
			context.write(title, pr);
		} else {
			title.set(key.toString());
			StringBuilder sb = new StringBuilder("");
			sb.append(links[0]);
			sb.append(String.valueOf(" "));
			sb.append(String.valueOf(((1 - alpha) + alpha * dangl) / N));
			sb.append("0");
			pr.set(sb.toString());
			context.write(title, pr);
		}
	}
}
