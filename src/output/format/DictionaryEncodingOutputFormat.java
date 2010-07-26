package output.format;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;

import output.writer.TriplesRecordWriter;

public class DictionaryEncodingOutputFormat extends MultipleTextOutputFormat<Text, Text> {
	
	protected RecordWriter<Text, Text> getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable progress) throws IOException {
		
		return new TriplesRecordWriter<Text, Text>(job, name, progress);
	}

	protected String generateFileNameForKeyValue(Text key, Text value, String name) {		
		return key.toString();
	}
}
