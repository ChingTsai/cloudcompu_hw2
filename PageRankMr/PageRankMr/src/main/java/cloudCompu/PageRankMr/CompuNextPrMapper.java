package cloudCompu.PageRankMr;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompuNextPrMapper extends
		Mapper<Text, Text, Text, StringArrayWritable> {
	private Text title = new Text();
	private StringArrayWritable pr = new StringArrayWritable();

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
				pr.setprePr(prepr / c);
				context.write(title, pr);
			}

			title.set(key.toString());
			pr.setprePr(prepr);
			pr.setPr(((1 - alpha) + alpha * dangl) / N);
			Text[] l = new Text[links.length - 2];
			for (int i = 2; i < links.length; i++) {
				l[i - 2].set(links[i]);
			}
			pr.set(l);
			context.write(title, pr);
		} else {
			title.set(key.toString());
			pr.setprePr(prepr);
			pr.setPr(((1 - alpha) + alpha * dangl) / N);
			context.write(title, pr);
		}
	}
}
