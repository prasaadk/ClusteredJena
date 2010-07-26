package jobs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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

import arq.query;

public class SPARQLQuery extends query {
	
	public static final String SPARQL_QUERY_RUNNER = "sparql.query.runner";
	public static final String QUERY_OBJECT = "sparql.query.object";

	public SPARQLQuery(String[] argv) {
		super(argv);		
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args){
		
		SPARQLQuery queryRunner = new SPARQLQuery(args);
		queryRunner.process();
		Query query = queryRunner.getQueryObject();		
		System.out.println(query);		
		MapRedQueryTool queryTool = new MapRedQueryTool(queryRunner,query);		
		
		System.out.println("initialized Query tool");
		
		String line = "[s]http://dbpedia.org/resource/Lithium_%28Sirius%29||[o]http://dbpedia.org/resource/Lithium_%28Sirius%29||";
		
		/*StringBuilder a = new StringBuilder();
		a.append(line);
		a.delete(a.length()-2, a.length());
		System.out.println(a);*/
		
		/*HashMap value = new HashMap();
		String[] tokens = line.split("(\\|\\|)");					
		String varKey = null;
        String varValue = null;
        
		for(int i=0;i<tokens.length;i++){
			String token = tokens[i];
			int varEnd = token.lastIndexOf(']');
			varKey = token.substring(1, varEnd);
			varValue = token.substring(varEnd+1,token.length());
			value.put(varKey,varValue);
		}*/
		
		
//		Op queryOp = Algebra.compile(query);
//		
//		OpProject project = (OpProject)queryOp;
//		OpBGP bgp = (OpBGP)project.getSubOp();
//		BasicPattern pattern = bgp.getPattern();
//		
//		ReorderTransformation reorder = new ReorderFixed();
//		pattern = reorder.reorder(pattern);
//		
//		HashMap<Triple, JoinGroup> tripleToGroup = new HashMap<Triple, JoinGroup>();
//		
//		List<Node> variableOrder = new ArrayList<Node>();
//		
//		HashMap<Node, JoinGroup> list = extractJoinTriples(pattern,tripleToGroup,variableOrder);
//		
//		Iterator<Node> i  = variableOrder.iterator();
//		
//		while(i.hasNext()){
//			Node node = i.next();
//			JoinGroup group = list.get(node);
//			System.out.println(node+" -> needs joins -> "+group);
//		}		
//		
//		Iterator<Triple> iter = pattern.iterator();
//		while(iter.hasNext()){
//			Triple triple = iter.next();
//			JoinGroup joinGroup = tripleToGroup.get(triple);
//			System.out.println("Triple:["+triple+"] --> ["+joinGroup+"]");
//		}
		
				
		try {
			int a = ToolRunner.run(new Configuration(), queryTool, args);
			System.out.println("Exit code:" + a);
		} catch (Exception e) {
			System.out.println("Exception:"+e);
		}
		
	}
	
//	// This method is developed to extract methods from Triples
//	public static HashMap<Node, JoinGroup> extractJoinTriples(BasicPattern pattern, HashMap<Triple, JoinGroup> tripleToGroup, List<Node> variableOrder){
//		
//		HashMap<Node,JoinGroup> varList = new HashMap();		
//		
//		Iterator<Triple> i = pattern.iterator();
//		int counter = 1;
//		while(i.hasNext()){
//			Triple triple = i.next();			
//			Node object = triple.getObject();
//			Node subject = triple.getSubject();
//			Node predicate = triple.getPredicate();
//			
//			if(object.isVariable()){
//				if(varList.containsKey(object)) {
//					JoinGroup joinGroup = varList.get(object);
//					joinGroup.add(counter,triple);					
//					tripleToGroup.put(triple, joinGroup);
//				}else{
//					JoinGroup joinGroup = new JoinGroup();
//					joinGroup.add(counter, triple);
//					varList.put(object,joinGroup);
//					tripleToGroup.put(triple, joinGroup);
//					variableOrder.add(object);
//				}
//			}
//			
//			if(subject.isVariable()){
//				if(varList.containsKey(subject)) {
//					JoinGroup joinGroup = varList.get(subject);
//					joinGroup.add(counter,triple);
//					tripleToGroup.put(triple, joinGroup);
//				}else{
//					JoinGroup joinGroup = new JoinGroup();
//					joinGroup.add(counter, triple);
//					varList.put(subject,joinGroup);
//					tripleToGroup.put(triple, joinGroup);
//					variableOrder.add(subject);
//				}
//			}
//			
//			if(predicate.isVariable()){
//				if(varList.containsKey(predicate)) {
//					JoinGroup joinGroup = varList.get(predicate);
//					joinGroup.add(counter,triple);
//					tripleToGroup.put(triple, joinGroup);
//				}else{
//					JoinGroup joinGroup = new JoinGroup();
//					joinGroup.add(counter, triple);
//					varList.put(predicate,joinGroup);
//					tripleToGroup.put(triple, joinGroup);
//					variableOrder.add(predicate);
//				}
//			}
//			counter++;
//		}
//		
//		return varList;
//	}
	
	public Query getQueryObject(){
		return modQuery.getQuery();
	}

}
