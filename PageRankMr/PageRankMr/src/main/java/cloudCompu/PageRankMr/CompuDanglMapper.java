package cloudCompu.PageRankMr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompuDanglMapper extends Mapper<Text, Text, Text, DoubleWritable> {
	private Text title = new Text();
	private DoubleWritable pr = new DoubleWritable();

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		String[] links = value.toString().split(" ");

		if (links.length == 1) {
			title.set("Dangle");
			pr.set(Double.parseDouble(links[0]));
			context.write(title, pr);
		}

	}
}
