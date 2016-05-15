package cloudCompu.PageRankMr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompuErrMapper extends Mapper<Text, Text, Text, Text> {
	private Text title = new Text();
	private DoubleWritable dis = new DoubleWritable();
	private Text d = new Text();

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		String[] detial = value.toString().split("&gt;");
		
		String[] par = detial[0].split(" ");
		title.set(key);
		d.set(par[0]+" "+par[1]);
		context.write(title, d);
		
			/*
			if (par[0] == null || par[1] == null) {
				title.set(key);
				dis.set(9.9);
				context.write(title, dis);
			} else {
				title.set("Sum");
				dis.set(Math.abs(Double.parseDouble(par[0])
						- Double.parseDouble(par[1])));
				context.write(title, dis);
			}
	*/
	}
}
