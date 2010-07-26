package mappers;

import input.readers.BaseRecordReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.Stack;

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


/**
 * 
 * @author prasaadk
 */
public class SelectionMapper extends MapReduceBase
implements Mapper<Text, Triple, Text, Text> {
	
	private static Logger log = LoggerFactory.getLogger(SelectionMapper.class);
	
	private String queryString = null;
	private Query query = null;
	private Op queryOp = null;
	private BasicPattern pattern = null; 
	@Override
	public void configure(JobConf job) {  
		
		this.queryString = job.get(Constants.QUERY_STRING);
		
		ModQueryIn queryMod = new ModQueryIn();
		queryMod.queryString = queryString;
		
		this.query = queryMod.getQuery();
		queryOp = Algebra.compile(query);
		
		OpProject project = (OpProject)queryOp;
		OpBGP bgp = (OpBGP)project.getSubOp();
		pattern = bgp.getPattern();
		
		System.out.println(queryOp);
		log.info(queryOp.toString());
		
	}
	
	@Override
	public void close() throws IOException { }

	@Override
	public void map(Text key, Triple value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		
		Iterator<com.hp.hpl.jena.graph.Triple> i = pattern.iterator();
		Text newKey = new Text();
		Text newValue = null;
		int patternNo=1;
		while(i.hasNext()){			
			com.hp.hpl.jena.graph.Triple patternItem = i.next();	
			
			if((newValue = checkEligibility(patternItem, value)) != null){				
				newKey.set(patternNo+"");
				break;
			}
			patternNo++;
		}		
		
		if(newValue!=null && newValue.getLength()>0) output.collect(newKey, newValue);
	}
	
	public Text checkEligibility(com.hp.hpl.jena.graph.Triple pattern, Triple input){		
		boolean equalityCheck = false;
		StringBuilder value = new StringBuilder();
		//System.out.println("Pattern Item:"+pattern);
		//System.out.println("Input Item:"+input);		
		if(pattern!=null && input!=null){
			
			if(pattern.getSubject().isConcrete()){ 
				if(pattern.subjectMatches(input.getSubject())){
					//System.out.println("Subject matched");
					equalityCheck = true;
				}
				else return null;
			}else if(pattern.getSubject().isVariable()){
				value.append("[").append(pattern.getSubject().getName()).append("]").append(input.getSubject()).append(Constants.DELIMIT);
			}
			
			if(pattern.getPredicate().isConcrete()){ 
				if(pattern.predicateMatches(input.getPredicate())){
					//System.out.println("Predicate matched");
					equalityCheck = true;
				}
				else return null;
			}else if(pattern.getPredicate().isVariable()){
				value.append("[").append(pattern.getPredicate().getName()).append("]").append(input.getPredicate()).append(Constants.DELIMIT);
			}
			
			if(pattern.getObject().isConcrete()){
				if(pattern.objectMatches(input.getObject())){
					//System.out.println("Object matched");
					equalityCheck = true;	
				}
				else return null;
			}else if(pattern.getObject().isVariable()){
				value.append("[").append(pattern.getObject().getName()).append("]").append(input.getObject()).append(Constants.DELIMIT);
			}
		}
		
		return new Text(value.toString());
	}
}

