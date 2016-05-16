package cloudCompu.PageRankMr;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

public class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text title = new Text();
	private Text link = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Document doc = null;
		try {
			doc = DocumentHelper.parseText(value.toString());
		} catch (DocumentException e) {

			e.printStackTrace();
		}

		String titleStr = doc.getRootElement().elementText("title");
		StringBuilder sb = new StringBuilder("");
		getAllText(doc.getRootElement(), sb);
		Matcher matcher = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])")
				.matcher(sb.toString());
		String[] links;
		title.set(titleStr);
		int clen;
		while (matcher.find()) {
			links = matcher.group().replaceAll("[\\[\\]]", "").split("[\\|#]");
			if (links.length > 0) {
				clen = links[0].length();
				if(clen==1){
					link.set(links[0].toUpperCase());
				}else{
					link.set(links[0].substring(0, 1).toUpperCase().concat(links[0].substring(1)));
				}
				
				context.write(link, title);
			}
		}
		link.set("&gt;");
		context.write(title, link);

	}

	public void getAllText(Element parent, StringBuilder sb) {

		for (Iterator i = parent.elementIterator(); i.hasNext();) {
			Element current = (Element) i.next();
			sb.append(current.getText());

			getAllText(current, sb);
		}

	}


}
