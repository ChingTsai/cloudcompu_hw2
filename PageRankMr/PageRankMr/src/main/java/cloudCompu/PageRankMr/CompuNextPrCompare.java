package cloudCompu.PageRankMr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompuNextPrCompare extends WritableComparator {
	protected CompuNextPrCompare() {
		super(Text.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2) {
		Text t1 = (Text) w1;
		Text t2 = (Text) w2;
		String[] s1, s2;
		s1 = t1.toString().split("&gt;");
		s2 = t2.toString().split("&gt;");
		Double err1 = Double.parseDouble(s1[1]);
		Double err2 = Double.parseDouble(s2[1]);
		if (Double.compare(err1, err2) == 0) {
			return s1[0].compareTo(s2[0]);
		} else {
			return err1.compareTo(err2);
		}
		/*
		 * int compare = s1[0].compareTo(s2[0]); if (compare == 0) { return
		 * Double.compare(Double.parseDouble(s1[1]), Double.parseDouble(s2[1]));
		 * } return compare;
		 */
	}
}