package de.hpi.mmds.cf.parsing;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

public class DumpWriter {

	private final TreeMap<Long, FileWriter> outs = new TreeMap<>();

	public DumpWriter(String fileName, int numPartitions, long max) throws IOException {
		for (int i = 0; i < numPartitions; i++) {
			File file = new File(fileName + i + ".txt");
			file.delete();
			outs.put(i * max / numPartitions, new FileWriter(file));
		}
	}

	public void write(Revision revision) {
		try {
			outs.floorEntry(revision.getArticleId()).getValue()
					.write(revision.getArticleId() + "," + revision.getUserId() + "," + revision.getTextLength() + "," + revision.isMinor() + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void close() throws IOException {
		for(Entry<Long, FileWriter> entry : outs.entrySet()) {
			entry.getValue().close();
		}
	}
}
