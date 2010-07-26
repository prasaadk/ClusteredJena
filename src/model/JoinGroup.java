package model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;

import utils.Constants;

import com.hp.hpl.jena.graph.Triple;

public class JoinGroup{
	
	HashMap<Integer, Triple> hashMap = new HashMap<Integer, Triple>();	
	List<Integer> tableList = new ArrayList<Integer>();
	boolean isJoined = false;
	
	public JoinGroup(){		
	}
	
	public void add(Integer number, Triple triple){
		hashMap.put(number, triple);		
		tableList.add(number);
	}
	
	public boolean isJoinable(){
		return (hashMap.size()>1) && !isJoined;
	}
	
	public int getSize(){
		return hashMap.size();
	}
	
	public String toString(){
		StringBuilder group = new StringBuilder();
		if(this.isJoinable()){			
			Iterator<Entry<Integer, Triple>> i = hashMap.entrySet().iterator();
			while(i.hasNext()){
				Entry<Integer, Triple> value = i.next();				
				group.append("["+value.getKey()+","+value.getValue().toString()+"],");
			}			
		}else{			
			group.append("Not Joinable");
		}
		return group.toString();
	}
	
	public String getTableList(HashMap<Integer, JoinGroup> tripleToGroup){
		return this.getTableList("",tripleToGroup);
	}
	
	public String getTableList(){
		return this.getTableList(Constants.DELIMIT);
	}
	
	public String getTableList(String delimiter,HashMap<Integer, JoinGroup> tripleToGroup){
		StringBuilder tableListString = new StringBuilder();
		Iterator<Integer> i = tableList.iterator();
		
		while(i.hasNext()){
			JoinGroup group = null;
			Integer table = i.next();			
			group = tripleToGroup.get(table);			
			if(group!=null && group.isJoined()) tableListString.append(group.getJoinedTableName()).append(Constants.DELIMIT);
			else tableListString.append("Triple_").append(table.toString()).append(Constants.DELIMIT);
		}
		tableListString.delete(tableListString.length()-2,tableListString.length());
		
		return tableListString.toString();
	}
	
	public String getTableList(String delimiter){
		System.out.println("With delimiter called:"+delimiter);
		StringBuilder tableListString = new StringBuilder();
		Iterator<Integer> i = tableList.iterator();
		while(i.hasNext()){
			Integer table = i.next();
			tableListString.append("Triple_").append(table.toString()).append(Constants.DELIMIT);
		}
		tableListString.delete(tableListString.length()-2,tableListString.length());
		
		return tableListString.toString();
	}
	
	public boolean isJoined(){
		return isJoined;
	}
	
	public void joined(){
		this.isJoined = true;
	}
	
	public String getJoinedTableName(){
		if(isJoined){
			return this.getTableList("");
		}
		return null;
	}
	
	public List<Path> getInputPaths(int joinPhaseCount){
		List<Path> inputPaths = new ArrayList<Path>();
		Iterator<Integer> i = tableList.iterator();
		while(i.hasNext()){
			Integer number = i.next();
			Path path = new Path(Constants.OUTPUT_DIR+joinPhaseCount+"/"+"Triple_"+number);
			inputPaths.add(path);
		}
		return inputPaths;		
	}
	
	public HashMap<Integer, JoinGroup> getJoinedList(){
		HashMap<Integer, JoinGroup> newMap = new HashMap<Integer, JoinGroup>();
		if(isJoined){
			Iterator<Integer> i = tableList.iterator();
			while(i.hasNext()){
				newMap.put(i.next(), this);
			}
		}
		return newMap;
	}
		
}