package mappers;

import input.format.LineIndexerInputFormat;
import input.format.LogicalTableInputFormat;
import input.format.NTriplesInputFormat;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import model.Triple;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import output.format.LineIndexerOutputFormat;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_URI;

public class DictionaryEncoder {

  public static class DictionaryEncoderMapper extends MapReduceBase
      implements Mapper<Text, Triple, Text, Text> {
	  
	private static Logger log = LoggerFactory.getLogger(DictionaryEncoderMapper.class);
	private static final String BLANK = "";
    public void map(Text key, Triple val,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

      Text subject = new Text();
      Text object = new Text();
      Text predicate = new Text();
      Text location = new Text();
      
      location.set(BLANK);
          
      subject.set(val.getSubject().toString());
      output.collect(subject, location);
      
      object.set(val.getObject().toString());
      output.collect(object, location);
      
      predicate.set(val.getPredicate().toString());
      output.collect(predicate, location);
      
    }    
    
  }



  public static class DictionaryEncoderReducer extends MapReduceBase
      implements Reducer<Text, Text, Text, Text> {
	  
	private static Logger log = LoggerFactory.getLogger(DictionaryEncoderReducer.class);
	
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

    	SecureRandom rnd = new SecureRandom();
    	Text randomKey = new Text(new BigInteger(16,rnd).toString());            
      
    	output.collect(randomKey, key);           
    	
    }
  }


  /**
   * The actual main() method for our program; this is the
   * "driver" for the MapReduce job.
   */
  public static void main(String[] args) {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(DictionaryEncoder.class);

    conf.setJobName("LineIndexer");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(conf, new Path("indexerinput"));
    FileOutputFormat.setOutputPath(conf, new Path("indexeroutput"));

    conf.setInputFormat(LineIndexerInputFormat.class);
    conf.setOutputFormat(LineIndexerOutputFormat.class);
    
    conf.setMapperClass(DictionaryEncoderMapper.class);
    conf.setReducerClass(DictionaryEncoderReducer.class);

    client.setConf(conf);    
    
    try {
      //JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}