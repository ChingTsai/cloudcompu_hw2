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
		String[] detial = value.toString().split("&gt;");
		String[] par = detial[0].split(" ");
		int len = Integer.parseInt(par[2]);
		String[] links;
		double prepr = Double.parseDouble(par[0]);
		double dangl = context.getConfiguration().getDouble("dangl", 1);
		double alpha = context.getConfiguration().getDouble("alpha", 0.85);
		Long N = context.getConfiguration().getLong("N", 1);
		if (len > 0) {
			links = detial[1].split("&lt;");
			for (int i = 0; i < len; i++) {
				title.set(links[i]);
				pr.set(String.valueOf(prepr / len) + " 0 -1");
				context.write(title, pr);
			}

			title.set(key.toString());
			StringBuilder sb = new StringBuilder("");
			sb.append(par[0]);
			sb.append(" ");
			sb.append(String.valueOf(((1 - alpha) + alpha * dangl) / N));
			sb.append(" ");
			sb.append(par[2]);
			sb.append("&gt;");
			sb.append(detial[1]);
			pr.set(sb.toString());
			context.write(title, pr);
		} else {
			title.set(key.toString());
			StringBuilder sb = new StringBuilder("");
			sb.append(par[0]);
			sb.append(" ");
			sb.append(String.valueOf(((1 - alpha) + alpha * dangl) / N));
			sb.append(" ");
			sb.append("0&gt;");
			pr.set(sb.toString());
			context.write(title, pr);
		}
	}
}
