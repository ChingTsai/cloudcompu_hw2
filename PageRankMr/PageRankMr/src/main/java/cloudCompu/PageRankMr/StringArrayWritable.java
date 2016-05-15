package cloudCompu.PageRankMr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class StringArrayWritable extends ArrayWritable {
	private double pr = 0;

	public void setPr(double pr) {
		this.pr = pr;
	}

	public double getPr() {
		return this.pr;
	}

	public StringArrayWritable() {
		super(Text.class);
	}

	public StringArrayWritable(Text[] texts) {
		super(LongWritable.class);
		set(texts);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeDouble(pr);
		;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		this.pr = in.readDouble();
	}
}
