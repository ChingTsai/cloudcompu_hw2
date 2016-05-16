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
		while (matcher.find()) {
			links = matcher.group().replaceAll("[\\[\\]]", "").split("[\\|#]");
			if (links.length > 0) {
				link.set(cap(links[0]));
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

	public String cap(String s) {

		char[] tmp = s.toCharArray();
		tmp[0] = Character.toUpperCase(tmp[0]);
		return tmp.toString();
	}
}
