package cloudCompu.PageRankMr;

import org.apache.hadoop.conf.Configuration;

public class job2 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setDouble("alpha", 0.85d);
	}
}
