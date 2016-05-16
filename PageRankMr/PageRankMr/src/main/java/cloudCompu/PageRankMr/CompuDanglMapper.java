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

		String[] detial = value.toString().split("&gt;");
		String[] par = detial[0].split(" ");
		if (Integer.parseInt(par[2]) == 0) {
			title.set("Sum");
			pr.set(Double.parseDouble(par[0]));
			context.write(key, pr);
		}

	}
}
