package de.hpi.mmds.cf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class DumpWriter {
	
	private final FileWriter out;
	
	
	public DumpWriter(File out) throws IOException {
		this.out = new FileWriter(out);
	}

	public void write(Revision revision) {
		try {
			out.write(revision.getArticleId() + "," + revision.getUserId() + "," + revision.getTimestamp() + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
