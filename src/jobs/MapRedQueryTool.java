package jobs;

import input.format.LogicalTableInputFormat;
import input.format.NTriplesInputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import mappers.LogicalTableJoinMapper;
import mappers.SelectionMapper;
import model.JoinGroup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.thrift.*;

import output.format.TriplesOutputFormat;

import reducers.LogicalTableJoinReducer;
import reducers.SelectionReducer;

import utils.Constants;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderFixed;
import com.hp.hpl.jena.sparql.engine.optimizer.reorder.ReorderTransformation;

import data.DataAccessLayer;

public class MapRedQueryTool extends Configured implements Tool {
	
	private static Logger log = LoggerFactory.getLogger(MapRedQueryTool.class);
	
	SPARQLQuery queryRunner = null;
	Query query = null;
	BasicPattern pattern = null;
	List<Node> variableOrder = null;
	HashMap<Node, JoinGroup> nodeToGroup = null;
	HashMap<Integer, JoinGroup> tripleToGroup = null;
	
	public MapRedQueryTool(SPARQLQuery queryRunner, Query query){
		this.queryRunner = queryRunner;
		this.query = query;
		
		Op queryOp = Algebra.compile(query);
		
		OpProject project = (OpProject)queryOp;
		OpBGP bgp = (OpBGP)project.getSubOp();
		this.pattern = bgp.getPattern();
		
		ReorderTransformation reorder = new ReorderFixed();
		this.pattern = reorder.reorder(pattern);
		
		tripleToGroup = new HashMap<Integer, JoinGroup>();
		
		variableOrder = new ArrayList<Node>();
		
		nodeToGroup = extractJoinTriples(pattern,tripleToGroup,variableOrder);	
		
		/*Iterator<Triple> iter = pattern.iterator();
		while(iter.hasNext()){
			Triple triple = iter.next();
			JoinGroup joinGroup = tripleToGroup.get(triple);
			System.out.println("Triple:["+triple+"] --> ["+joinGroup+"]");
		}*/
		
		
	}
	@Override
	public int run(String[] args) throws Exception {
		
		selectionPhase(args);
		
		// Next Join phase....
		HashMap<Integer, JoinGroup> joinedGroup = new HashMap<Integer, JoinGroup>();
		
		Iterator<Node> i  = variableOrder.iterator();
		int joinPhaseCount = 1;
		while(i.hasNext()){
			
			Node node = i.next();
			JoinGroup group = nodeToGroup.get(node);
			if(group.isJoinable()){
				String joinValue = node.getName();		
				//System.out.println("JoinValue:"+joinValue);
			
				int noOfTables = group.getSize();
				//System.out.println("NoOfTables:"+noOfTables);
				
				String tableList = group.getTableList(joinedGroup);
				//System.out.println("TableList:"+tableList);
				
				List<Path> inputPaths = group.getInputPaths(joinPhaseCount);
						
				joinPhase(joinPhaseCount, joinValue, noOfTables, tableList,inputPaths);
			
				//System.out.println(node+" -> needs joins -> "+group);
				
				joinPhaseCount++;
			}
			group.joined();
			
			joinedGroup.putAll(group.getJoinedList());
			
		}
		
		return 0;
	}
		
	// This is Join phase
	public int joinPhase(int joinPhase,String joinValue, int noOfTables, String tableNames, List<Path> inputPaths) throws IOException{
		
		JobConf conf = new JobConf(getConf(), getClass());
		
		conf.setJobName("Map Reduce SPARQL Query Join variable + s");
		
		System.out.println("conf:"+conf);
		if (conf == null) {
			return -1;
		}		
				
		conf.set(Constants.JOIN_VALUE, joinValue);
		conf.set(Constants.NO_OF_TABLES, noOfTables+"");
		
		
		conf.set(Constants.LIST_OF_TABLES, tableNames);
				
		Iterator<Path> i = inputPaths.iterator();
		
		while(i.hasNext()){
			Path inputPath = i.next();
			FileInputFormat.addInputPath(conf, inputPath);
		}
				
		FileOutputFormat.setOutputPath(conf, new Path(Constants.OUTPUT_DIR+(joinPhase+1)+"/"));
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);		
		
		conf.setMapperClass(LogicalTableJoinMapper.class);
		conf.setReducerClass(LogicalTableJoinReducer.class);
		
		conf.setInputFormat(LogicalTableInputFormat.class);
		conf.setOutputFormat(TriplesOutputFormat.class);
		
		JobClient.runJob(conf);
		
		return 0;
	}
	
	// This is selection phase
	private int selectionPhase(String[] args) throws IOException {
		
		System.out.println("Start run method..");
		JobConf conf = new JobConf(getConf(), getClass());
	
		System.out.println("conf:"+conf);
		if (conf == null) {
			return -1;
		}		
		conf.set(Constants.QUERY_STRING, query.toString());
		conf.set(Constants.ARGS,args.toString());
		conf.setJobName("Map Reduce SPARQL Query");
		
		System.out.println("Adding config values");
		
		List<String> listOfkeys = getListOfWords(pattern);
		
		String inputDocList = fetchDocumentList(listOfkeys,new Path(queryRunner.getArg("graph").getValue()));
		
		FileInputFormat.addInputPaths(conf, inputDocList);
		FileOutputFormat.setOutputPath(conf, new Path("output1/"));
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(SelectionMapper.class);
		conf.setReducerClass(SelectionReducer.class);
		
		conf.setInputFormat(NTriplesInputFormat.class);
		conf.setOutputFormat(TriplesOutputFormat.class);		
		
		
		System.out.println("Starting JobClient.runJob");
		JobClient.runJob(conf);
		System.out.println("Stop run method..");
		
		return 0;
		
	}
	
	public String fetchDocumentList(List<String> listKeys,Path inputPath){		
		DataAccessLayer access = new DataAccessLayer();		
		access.init();
		Map<String,Column> map = access.fetch(listKeys);
		
		StringBuilder documentList = new StringBuilder();
		
		Set<String> file = new HashSet<String>();
		Iterator<String> iter = listKeys.iterator();
		while(iter.hasNext()){
			Column column = map.get(iter.next());
			String[] tokens = new String(column.getValue()).split("[,]");
			for(int i=0;i<tokens.length;i++){
				if(!file.contains(tokens[i])){
					documentList.append(inputPath.toString()).append("/").append(tokens[i].trim()).append(",");
					file.add(tokens[i]);
				}
			}			
		}
		documentList.deleteCharAt(documentList.length()-1);
		System.out.println("List of InputPaths: "+documentList);
		return documentList.toString();	
		
	}
	
	public List<String> getListOfWords(BasicPattern pattern){
		List<String> listKeys = new ArrayList<String>();
		Iterator<Triple> i = pattern.getList().iterator();
		while(i.hasNext()){
			
			Triple triple = i.next();
			
			String key = null;
			
			if((key=getKey(triple.getSubject()))!=null) listKeys.add(key);
			if((key=getKey(triple.getPredicate()))!=null) listKeys.add(key);
			if((key=getKey(triple.getObject()))!=null) listKeys.add(key);
			
		}
		
		return listKeys;
		
	}
	
	public String getKey(Node node){
		StringBuilder key = new StringBuilder();
		if(node.isConcrete()){
			key.append("[").append(node.toString()).append("]");
			return key.toString();
		}
		return null;
	}
	
	// This method is developed to extract methods from Triples
	public static HashMap<Node, JoinGroup> extractJoinTriples(BasicPattern pattern, HashMap<Integer, JoinGroup> tripleToGroup, List<Node> variableOrder){
		
		HashMap<Node,JoinGroup> varList = new HashMap();		
		
		Iterator<Triple> i = pattern.iterator();
		int counter = 1;
		while(i.hasNext()){
			Triple triple = i.next();			
			Node object = triple.getObject();
			Node subject = triple.getSubject();
			Node predicate = triple.getPredicate();
			
			if(subject.isVariable()){
				if(varList.containsKey(subject)) {
					JoinGroup joinGroup = varList.get(subject);
					joinGroup.add(counter,triple);
					tripleToGroup.put(counter, joinGroup);
				}else{
					JoinGroup joinGroup = new JoinGroup();
					joinGroup.add(counter, triple);
					varList.put(subject,joinGroup);
					tripleToGroup.put(counter, joinGroup);
					variableOrder.add(subject);
				}
			}			

			if(predicate.isVariable()){
				if(varList.containsKey(predicate)) {
					JoinGroup joinGroup = varList.get(predicate);
					joinGroup.add(counter,triple);
					tripleToGroup.put(counter, joinGroup);
				}else{
					JoinGroup joinGroup = new JoinGroup();
					joinGroup.add(counter, triple);
					varList.put(predicate,joinGroup);
					tripleToGroup.put(counter, joinGroup);
					variableOrder.add(predicate);
				}
			}
			
			if(object.isVariable()){
				if(varList.containsKey(object)) {
					JoinGroup joinGroup = varList.get(object);
					joinGroup.add(counter,triple);					
					tripleToGroup.put(counter, joinGroup);
				}else{
					JoinGroup joinGroup = new JoinGroup();
					joinGroup.add(counter, triple);
					varList.put(object,joinGroup);
					tripleToGroup.put(counter, joinGroup);
					variableOrder.add(object);
				}
			}			
			counter++;
		}
		
		return varList;
	}
}