package cloudCompu.PageRankMr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompuDanglMapper extends
		Mapper<Text, StringArrayWritable, Text, DoubleWritable> {
	private Text title = new Text();
	private DoubleWritable pr = new DoubleWritable();

	public void map(Text key, StringArrayWritable value, Context context)
			throws IOException, InterruptedException {
		if (value.toStrings().length == 0) {
			title.set("Dangle");
			pr.set(value.getPr());
			context.write(title, pr);
		}

	}
}
