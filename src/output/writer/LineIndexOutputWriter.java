package output.writer;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.lucene.index.IndexFileNameFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.DataAccessLayer;

public class LineIndexOutputWriter<K, V> implements RecordWriter<K, V> {
	
	 Logger log = LoggerFactory.getLogger(LineIndexOutputWriter.class);
	 private OutputStreamWriter out = null;
	 //private FSDataOutputStream out = null;
	 private Path file = null;
	 
	 DataAccessLayer dataAccess = null;
	 
	 public LineIndexOutputWriter(JobConf job, String name, Progressable progress) {
		 try {
			file = new Path(FileOutputFormat.getOutputPath(job), name);
			
		    FileSystem fs = file.getFileSystem(job);
		    
		    
		    out = new OutputStreamWriter(fs.create(file, progress));
		   
		    if(dataAccess == null){
		    	dataAccess = new DataAccessLayer();
		    	dataAccess.init();
		    }
		    
		    //out = ;
		    
		    
		} catch (Exception e) {			
			log.error("Couldn't create file", e);
		}
	 }

	@Override
	public void close(Reporter reporter) throws IOException {
		log.info("close called..");
		//out.close();
		dataAccess.close();		
	}

	@Override
	public void write(K key, V value) throws IOException {		
		//out.write(value.toString()+"\n");
		dataAccess.write(key, value);			
	}
}
