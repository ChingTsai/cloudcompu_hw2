package cloudCompu.PageRankMr;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PruneReduce extends Reducer<Text, Text, Text, StringArrayWritable> {
	private Text title = new Text();
	private StringArrayWritable links = new StringArrayWritable();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<String> link = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		long N = context.getConfiguration().getLong("N", 1);
		for (Text val : values) {
			if (!val.toString().equals("&gt"))
				link.add(val.toString());
			// sb.append(", " + val);
		}

		title.set(key);
		links.setPr((1d / N));
		links.set(link.toArray(new StringArrayWritable[link.size()]));
		context.write(title, links);

	}
}