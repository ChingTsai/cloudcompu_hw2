package cloudCompu.PageRankMr;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParseReduce extends Reducer<Text, Text, Text, Text> {
	private Text title = new Text();
	private Text link = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<String> titles = new ArrayList<String>();

		boolean miss = true;
		for (Text val : values) {
			if (val.toString().equals("&gt")) {
				miss = false;
			} else {
				titles.add(val.toString());
			}
		}
		if (!miss) {
			for (String s : titles) {
				title.set(s);
				link.set(key);
			}
			context.write(title, link);
		}

	}
}
