package cloudCompu.PageRankMr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReduce extends Reducer<Text, Text, Text, Text> {
	private Text title = new Text();
	private Text pr = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String[] detial = key.toString().split("&gt;");

		title.set(detial[0]);
		pr.set(detial[1]);
		context.write(title, pr);
	}
}
