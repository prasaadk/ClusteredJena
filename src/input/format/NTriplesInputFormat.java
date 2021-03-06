package input.format;

import input.readers.NTripleReader;

import java.io.IOException;

import model.Triple;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;



public class NTriplesInputFormat extends MultiFileInputFormat<Text, Triple> {

	@Override
	public RecordReader<Text, Triple> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
		return new NTripleReader(job,(MultiFileSplit)split);
	}	
	
}