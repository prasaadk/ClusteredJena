package mappers;

import input.readers.BaseRecordReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Stack;
import java.util.Map.Entry;

import model.Triple;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Constants;

import arq.cmdline.ModQueryIn;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitor;
import com.hp.hpl.jena.sparql.algebra.op.OpAssign;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpConditional;
import com.hp.hpl.jena.sparql.algebra.op.OpDatasetNames;
import com.hp.hpl.jena.sparql.algebra.op.OpDiff;
import com.hp.hpl.jena.sparql.algebra.op.OpDisjunction;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpGroupAgg;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLabel;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpList;
import com.hp.hpl.jena.sparql.algebra.op.OpMinus;
import com.hp.hpl.jena.sparql.algebra.op.OpNull;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpPath;
import com.hp.hpl.jena.sparql.algebra.op.OpProcedure;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpPropFunc;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpTable;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.main.OpExecutor;



public class LogicalTableJoinMapper extends MapReduceBase
implements Mapper<Text, HashMap<String, String>, Text, Text> {
	
	private static Logger log = LoggerFactory.getLogger(LogicalTableJoinMapper.class);
	
	private String joinValue = null;
	private Query query = null;
	private Op queryOp = null;
	private BasicPattern pattern = null; 
	@Override
	public void configure(JobConf job) {  
		
		this.joinValue = job.get(Constants.JOIN_VALUE);		
		
	}
	
	@Override
	public void close() throws IOException { }

	@Override
	public void map(Text key, HashMap<String,String> value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {		
		Text outputKey = new Text();
		outputKey.set(generateOutputKey(value));		
		//log.info("OutputKey:"+outputKey);		
				
		
		Text outputValue = new Text();
		outputValue.set(generateOutputValue(key,value));		
		//log.info("OutputValue:"+outputValue);		
		
		output.collect(outputKey, outputValue);
	}
	
	public String generateOutputKey(HashMap<String,String> value){
		StringBuilder tempKey = new StringBuilder();		
		tempKey.append("[").append(joinValue).append("]").append(value.get(joinValue));		
		return tempKey.toString();		
	}
	
	public String generateOutputValue(Text key,HashMap<String,String> value){
		
		Iterator<Entry<String,String>> i = value.entrySet().iterator();
		StringBuilder builder = new StringBuilder();
		while(i.hasNext()){
			Entry<String,String> entry = i.next();
			String varKey = entry.getKey();
			String varValue = entry.getValue();			
			
			if(!joinValue.equals(varKey)){
				builder.append("{").append(key).append("}").append("[").append(varKey).append("]").append(value.get(varKey)).append(Constants.DELIMIT);				
			}			
		}
		
		if(builder.length()==0){
			builder.append("{").append(key).append("}");			
		}
		
		return builder.toString();
	}
}

