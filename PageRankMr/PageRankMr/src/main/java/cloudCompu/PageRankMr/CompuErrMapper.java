package cloudCompu.PageRankMr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompuErrMapper extends Mapper<Text, Text, Text, DoubleWritable> {
	private Text title = new Text();
	private DoubleWritable dis = new DoubleWritable();

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		String[] detial = value.toString().split("&gt;");

			String[] par = detial[0].split(" ");
			if(par.length == 1) {
				title.set(key);
				dis.set(9.9);
				context.write(title, dis);
			}else{
			title.set("Sum");
			dis.set(Math.abs(Double.parseDouble(par[0])
					- Double.parseDouble(par[1])));
			context.write(title, dis);
			}
	}

}
