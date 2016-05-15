package cloudCompu.PageRankMr;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PruneReduce extends Reducer<Text, Text, Text, Text> {
	private Text title = new Text();
	private Text link = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<String> titles = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();

		for (Text val : values) {

			// titles.add(val.toString());
			sb.append(", " + val);
		}

		title.set(key);
		link.set(sb.toString());
		context.write(title, link);

	}
}