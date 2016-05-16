package cloudCompu.PageRankMr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Text, Text, Text, Text> {
	private Text title = new Text();
	private Text pr = new Text();

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		String[] detial = value.toString().split("&gt;");
		String[] par = detial[0].split(" ");
		title.set(key + "&gt;" + par[0]);
		pr.set(" ");
		context.write(title, pr);
	}
}
