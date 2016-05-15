package cloudCompu.PageRankMr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class StringArrayWritable extends ArrayWritable {
	private double prepr = 0;
	private double pr = 0;
	private int len = 0;

	public void setLen(int len) {
		this.len = len;
	}

	public int getLen() {
		return this.len;
	}

	public void setPr(double pr) {
		this.pr = pr;
	}

	public double getPr() {
		return this.pr;
	}

	public void setprePr(double prepr) {
		this.prepr = prepr;
	}

	public double getprePr() {
		return this.prepr;
	}

	public StringArrayWritable() {
		super(Text.class);
	}

	public StringArrayWritable(Text[] texts) {
		super(Text.class);
		set(texts);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeDouble(pr);
		out.writeDouble(prepr);
		out.writeInt(len);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		this.pr = in.readDouble();
		this.prepr = in.readDouble();
		this.len = in.readInt();
	}
}
