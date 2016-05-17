package cloudCompu.PageRankMr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PruneReduce extends Reducer<Text, Text, Text, Text> {
	private Text title = new Text();
	private Text links = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// ArrayList<Text> link = new ArrayList<Text>();
		StringBuilder sb = new StringBuilder();
		long N = context.getConfiguration().getLong("N", 1);
		int len = 0;
		for (Text val : values) {
			if (!val.toString().equals("&gt;")) {
				sb.append(val+"&lt;" );
				len++;
			}

		}

		title.set(key);

		links.set(String.valueOf(1d / N) + " 1 " + len + "&gt;" + sb.toString());
		context.write(title, links);

	}
}