package mappers;

import input.format.LineIndexerInputFormat;
import input.format.LogicalTableInputFormat;
import input.format.NTriplesInputFormat;

import java.io.IOException;
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

public class LineIndexer {

  public static class LineIndexMapper extends MapReduceBase
      implements Mapper<Text, Triple, Text, Text> {
	private static Logger log = LoggerFactory.getLogger(LineIndexMapper.class);
	
    public void map(Text key, Triple val,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

      Text subject = new Text();
      Text object = new Text();
      Text predicate = new Text();
      Text location = new Text();
      
      location.set(key);
          
      subject.set(getValueInKeyFormat(val.getSubject()));
      output.collect(subject, location);
      
      object.set(getValueInKeyFormat(val.getObject()));
      output.collect(object, location);
      
      predicate.set(getValueInKeyFormat(val.getPredicate()));
      output.collect(predicate, location);
      
    }    
        
    public Text getValueInKeyFormat(Node value){
    	StringBuilder valueString = new StringBuilder();    	
    	String replacedValue = value.toString().replace(' ', '_');
    	valueString.append("[").append(replacedValue).append("]");
    	return new Text(valueString.toString());	
    }
    
  }



  public static class LineIndexReducer extends MapReduceBase
      implements Reducer<Text, Text, Text, Text> {
	  
	private static Logger log = LoggerFactory.getLogger(LineIndexReducer.class);
	
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

      boolean first = true;
      StringBuilder toReturn = new StringBuilder();
      HashSet newSet = new HashSet();
      while (values.hasNext()){
        newSet.add(values.next().toString());        
      }
      log.info("Key:"+key+", Values:"+newSet.toString());
      Iterator i = newSet.iterator();
      while(i.hasNext()){
    	  if (!first)
              toReturn.append(",");
          first=false;
          toReturn.append(i.next().toString());
      }
      
      output.collect(key, new Text(toReturn.toString()));            
    }
  }


  /**
   * The actual main() method for our program; this is the
   * "driver" for the MapReduce job.
   */
  public static void main(String[] args) {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(LineIndexer.class);

    conf.setJobName("LineIndexer");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(conf, new Path("indexerinput"));
    FileOutputFormat.setOutputPath(conf, new Path("indexeroutput"));

    conf.setInputFormat(LineIndexerInputFormat.class);
    conf.setOutputFormat(LineIndexerOutputFormat.class);
    
    conf.setMapperClass(LineIndexMapper.class);
    conf.setReducerClass(LineIndexReducer.class);

    client.setConf(conf);

    try {
      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}