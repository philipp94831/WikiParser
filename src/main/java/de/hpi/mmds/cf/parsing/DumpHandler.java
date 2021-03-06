package de.hpi.mmds.cf.parsing;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.Stack;

import javax.xml.namespace.QName;

import com.github.philipp94831.stax2parser.DefaultHandler;

public class DumpHandler extends DefaultHandler {

	private Stack<String> parents;
	private StringBuilder buf;
	private boolean inText = false;
	private long currentArticle;
	private Revision currentRevision;
	private final DumpWriter testWriter;
	private final DumpWriter trainingWriter;
	private final Date threshold;
	private int currentNamespace;
	private final Set<Long> bots;
	private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");

	public DumpHandler(DumpWriter testWriter, DumpWriter trainingWriter, Date threshold, Set<Long> bots) {
		this.testWriter = testWriter;
		this.trainingWriter = trainingWriter;
		this.threshold = threshold;
		this.bots = bots;
	}

	@Override
	public void startDocument() {
		parents = new Stack<>();
	}
	
	@Override
	public void attribute(QName qName, String value) {
		if(isInText() && qName.equals("bytes")) {
			currentRevision.setTextLength(Integer.parseInt(value));
		}
	}

	@Override
	public void endElement(QName qname) {
		if (isInId()) {
			currentArticle = Long.parseLong(buf.toString());
		}
		if (isInNamespace()) {
			currentNamespace = Integer.parseInt(buf.toString());
		}
		if (isInContributorId()) {
			currentRevision.setUserId(Long.parseLong(buf.toString()));
		}
		if (isInTimestamp()) {
			try {
				Date timestamp = DATE_FORMAT.parse(buf.toString());
				currentRevision.setTimestamp(timestamp);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (isInRevision()) {
			if (currentRevision.getUserId() != 0 && currentNamespace == 0 && !bots.contains(currentRevision.getUserId())) {
				if (currentRevision.getTimestamp().compareTo(threshold) < 0) {
					trainingWriter.write(currentRevision);
				} else {
					testWriter.write(currentRevision);
				}
			}
			currentRevision = null;
		}
		parents.pop();
	}

	@Override
	public void characters(String ch) {
		if (inText) {
			buf.append(ch);
		}
	}

	@Override
	public void startElement(QName qname) {
		parents.push(qname.getLocalPart());
		if (isInId() || isInContributorId() || isInTimestamp() || isInNamespace()) {
			inText = true;
			buf = new StringBuilder();
		}
		if (isInRevision()) {
			currentRevision = new Revision(currentArticle);
		}
		if(isInMinor()) {
			currentRevision.setMinor(true);
		}
	}

	private boolean isInNamespace() {
		return parents.peek().equals("ns") && parents.elementAt(parents.size() - 2).equals("page");
	}

	private boolean isInContributorId() {
		return parents.peek().equals("id") && parents.elementAt(parents.size() - 2).equals("contributor");
	}

	private boolean isInId() {
		return parents.peek().equals("id") && parents.elementAt(parents.size() - 2).equals("page");
	}

	private boolean isInTimestamp() {
		return parents.peek().equals("timestamp") && parents.elementAt(parents.size() - 2).equals("revision");
	}

	private boolean isInText() {
		return parents.peek().equals("text") && parents.elementAt(parents.size() - 2).equals("revision");
	}

	private boolean isInMinor() {
		return parents.peek().equals("minor") && parents.elementAt(parents.size() - 2).equals("revision");
	}

	private boolean isInRevision() {
		return parents.peek().equals("revision");
	}

	public void close() throws IOException {
		testWriter.close();
		trainingWriter.close();
	}
}
