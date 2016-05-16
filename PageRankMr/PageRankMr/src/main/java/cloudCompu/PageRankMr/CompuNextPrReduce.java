package cloudCompu.PageRankMr;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CompuNextPrReduce extends Reducer<Text, Text, Text, Text> {
	private Text title = new Text();
	private Text link = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder("");

		double pr = 0.0d;
		String prepr = null;
		double outpr = 0.0d;
		String[] detial;
		String[] par = null;
		int len = 0;
		int newlen = 0;
		for (Text val : values) {
			detial = val.toString().split("&gt;");
			par = detial[0].split(" ");
			len = Integer.parseInt(par[2]);

			if (len != -1) {
				if (len > 0)
					sb.append("&gt;" + detial[1]);
				else
					sb.append("&gt;");
				pr = Double.parseDouble(par[1]);
				prepr = par[0];
			} else {
				outpr += Double.parseDouble(par[0]);
				newlen++;
			}
		}
		sb.insert(0, newlen);
		sb.insert(0, " ");
		sb.insert(0, prepr);
		sb.insert(0, " ");
		sb.insert(0, String.valueOf(pr + outpr));

		title.set(key);
		link.set(sb.toString());
		context.write(title, link);
	}
}
