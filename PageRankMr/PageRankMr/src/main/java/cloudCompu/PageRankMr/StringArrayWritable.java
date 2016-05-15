package cloudCompu.PageRankMr;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class StringArrayWritable extends ArrayWritable {
	public StringArrayWritable() {
		super(Text.class);
	}

	public StringArrayWritable(Text[] texts) {
		super(LongWritable.class);
		set(texts);
	}
}
