package input.readers;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Constants;

import com.hp.hpl.jena.graph.Node;
import model.Triple;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFErrorHandler;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.impl.RDFDefaultErrorHandler;
import com.hp.hpl.jena.shared.SyntaxError;
import com.hp.hpl.jena.sparql.util.graph.GraphFactory;

public class LogicalTableTripleReader extends LogicalTableBaseRecordReader<Text,HashMap<String, String>>{
	
	private static final String EMPTY = "";
	
	private int recordCounter = 0;
	private boolean inErr = false;
	private int errCount = 0;
	private static final int sbLength = 200;
	
	private Hashtable<String, Resource> anons = new Hashtable<String, Resource>();

	private static Logger log = LoggerFactory.getLogger(LogicalTableTripleReader.class);
	
	private RDFErrorHandler errorHandler = new RDFDefaultErrorHandler();
	
	/**
     * Already with ": " at end for error messages.
     */
    private String base;
	
	public LogicalTableTripleReader(Configuration job, MultiFileSplit input) throws IOException {
		super(job, input);		
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public HashMap<String, String> createValue() {
		//System.out.println("selfdebug: create Triple value");		
		//log.info("Fresh new hashMap key created..");
		HashMap<String, String> value = new HashMap<String, String>();		
		//System.out.println("selfdebug: Triple created");
		return value;
	}

	@Override
	public boolean next(Text key, HashMap<String, String> value) throws IOException {
		value.clear();	
		String line = null;
		do {
			if (in != null) {				
				
				try {
					line = in.readLine();
					
				} catch (Exception e) {
					log.warn("Failed reading a line. Go to the next file");
				}
				
				if (line != null) {
					key.set(getCurrentFileName());
					
					String[] tokens = line.split(Constants.REGEX);					
					String varKey = null;
			        String varValue = null;
			        
					for(int i=0;i<tokens.length;i++){
						String token = tokens[i];
						int varEnd = token.lastIndexOf(']');
						//log.info("Token" + token +", substring 1 to "+varEnd);
						varKey = token.substring(1, varEnd);
						//log.info("Into hashmap key:"+varKey);
						varValue = token.substring(varEnd+1,token.length());
						//log.info("Into hashmap value:"+varValue);
						if(varKey.length()>0 && varValue.length()>0){
							value.put(varKey,varValue);
						}
					}
					
					return true;
				}			
			}
			
			if (!openNextFile()){				
				System.out.println("openNextFile() returned False");
				return false;
			}
			System.out.println("openNextFile() returned True");
				
		} while (line==null);
		
		return false;
	}
}
