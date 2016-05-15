package cloudCompu.PageRankMr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PruneMapper extends Mapper<Text, Text, Text, Text> {
	private Text title = new Text();
	private Text link = new Text();

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		title.set(key);
		link.set(value);
		context.write(title, link);
	}
}
