package reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import mappers.SelectionMapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Constants;

public class LogicalTableJoinReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private static Logger log = LoggerFactory.getLogger(LogicalTableJoinReducer.class);
	
	String[] tables = null;
	int noOfTables = 0;
	
	public void configure(JobConf job) {
		String noOfTables = job.get(Constants.NO_OF_TABLES);
		String listOfTables = job.get(Constants.LIST_OF_TABLES);
		
		if(listOfTables!=null){
			tables = listOfTables.split(Constants.REGEX);			
		}
		
		log.info("Incoming tables1:"+tables[0]);
		log.info("Incoming tables2:"+tables[1]);
	}
	
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		//log.info("New reducer called..");
		HashSet<String> files = new HashSet<String>();
		//System.out.println("Reducing.."+key);	
		//log.info("Reducing.."+key);
		
		ArrayList<String> remainingStringList = new ArrayList<String>();
		
		while(values.hasNext()){
			
			String value = values.next().toString();			
			//log.info("Value:" + value);
			if(value.length()>0){			
				int fileNameEnds = value.lastIndexOf('}');
				String tableName = value.substring(1,fileNameEnds);
				
				//log.info("Received tablename:"+tableName);
				
				files.add(tableName);
				
				String restOfString = value.substring(fileNameEnds+1,value.length());
				if(restOfString!=null && restOfString.length()>1){
					remainingStringList.add(restOfString);
				}				
			}
		}	
		
							
        String newTableName = null;
        if((newTableName = validGroup(files))!=null){			        	
        	//log.info("New tableName:"+ newTableName);
        	for(int i=0;i<remainingStringList.size();i++){
        		StringBuilder newValue = new StringBuilder();
        		newValue.append(key).append(Constants.DELIMIT).append(remainingStringList.get(i));
        		output.collect(new Text(newTableName), new Text(newValue.toString()));
        	}
        }
        //log.info("New reducer ends..");
	}
	
	private String validGroup(HashSet<String> files){
		StringBuilder tableName = new StringBuilder();
		for(int i=0;i<tables.length;i++){
        	if(!files.contains(tables[i])){
        		return null;
        	}else{
        		tableName.append(tables[i]);
        	}
        }
		return tableName.toString();
	}

}
