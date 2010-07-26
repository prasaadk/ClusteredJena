package input.readers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.util.graph.GraphFactory;

import utils.Constants;
import utils.IStream;

abstract public class LogicalTableBaseRecordReader<K, V> implements RecordReader<K, V> {

	private static Logger log = LoggerFactory.getLogger(LogicalTableBaseRecordReader.class);
	
	protected Configuration job = null;
	protected MultiFileSplit split = null;
	protected long currentPosition = 0;
	protected FSDataInputStream currentFileStream = null;

	protected BufferedReader in = null;
	protected int currentFile = 0;
	protected String fileName = null;
	
	protected boolean openNextFile() {
		System.out.println("start openNextFile..");
		System.out.println("currentFile:"+currentFile+", split.getNumPaths()"+split.getNumPaths());
		
		if (currentFile < split.getNumPaths()) {
			do {
				Path path = null;
				try {
					if (in != null) {
						in.close();
						currentFileStream.close();
						currentPosition += split.getLength(currentFile - 1);
						System.out.println("currentPosition:"+currentPosition);
					}
					in = null;
					path = split.getPath(currentFile);
					currentFileStream = path.getFileSystem(job).open(path);					
					in = new BufferedReader(new InputStreamReader(currentFileStream,"UTF-8"));
					fileName = path.getName();
					System.out.println("filename:"+fileName);
				} catch (Exception e) {
					if (path != null)
						log.info("Failed opening file: " + path, e);
					else
						log.info("Failed closing file", e);
				}
				++currentFile;
			} while (in == null && currentFile < split.getNumPaths());
			
			if (in != null)
				return true;
		}
		System.out.println("end openNextFile..");
		return false;
	}	
	
	public LogicalTableBaseRecordReader(Configuration job, MultiFileSplit input) throws IOException {
		split = input;
		this.job = job;		
		openNextFile();
	}

	@Override
	public void close() throws IOException {
		if (in != null)
			in.close();
		if (currentFileStream != null)
			currentFileStream.close();
	}

	@Override
	public long getPos() throws IOException {
		return currentPosition;
	}

	@Override
	public float getProgress() throws IOException {
		return (float)currentPosition / (float)split.getLength();
	}
	
	public String getCurrentFileName() {
		return fileName;
	}
}
