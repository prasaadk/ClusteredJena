package data;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.service.PoolExhaustedException;

import org.apache.cassandra.thrift.*;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import output.writer.LineIndexOutputWriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static me.prettyprint.cassandra.utils.StringUtils.string;

/**
 * This is DataAccessLayer to access Cassandra
 *
 * @author Prasad Kulkarni
 */
public class DataAccessLayer<K,V> {
	
	Logger log = LoggerFactory.getLogger(DataAccessLayer.class);
	
	CassandraClientPool pool = null;
	CassandraClient client = null; 
	Keyspace ks = null;
	ColumnPath columnPath = null;
	
	public void close(){
		log.info("pool release called..");
		try {
			pool.releaseClient(client);
			pool = null;        
			client = null;
			ks = null;
			columnPath = null;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void init(){
		log.info("init called..");
		pool = CassandraClientPoolFactory.INSTANCE.get();
        try {
			client = pool.borrowClient("localhost", 9160);
			ks = client.getKeyspace("Keyspace1");
			columnPath = new ColumnPath("Standard2");
			columnPath.setColumn("DocNameList".getBytes());
			
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PoolExhaustedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void write(K key,V value){
		
        try {        	
        	if(key!=null && value!=null){
        		log.info("Writing key:"+key+", value:"+value);
        		ks.insert(key.toString(), columnPath, value.toString().getBytes());
        		log.info("Written key:"+key+", value:"+value);
        	}
		} catch (InvalidRequestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnavailableException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimedOutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public Map<String, Column> fetch(List<String> keys){
		long startTime = System.currentTimeMillis();
		Map map = new HashMap();  
		
		try {			
			map = ks.multigetColumn(keys, columnPath);
		} catch (IllegalArgumentException e1) {
			map = null;
			e1.printStackTrace();
		} catch (TException e1) {
			map = null;
			e1.printStackTrace();
		} catch (InvalidRequestException e) {
			map = null;
			e.printStackTrace();
		} catch (UnavailableException e) {
			map = null;
			e.printStackTrace();
		} catch (TimedOutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        long endTime = System.currentTimeMillis();
        log.info("Time to fetch list of keys:"+(endTime-startTime));
        return map;
	}
	
	public String fetch(String key){
		long startTime = System.currentTimeMillis();
		Column c;  
		
		try {			
			c = ks.getColumn(key, columnPath);
		} catch (InvalidRequestException e) {
			c = null;
			e.printStackTrace();
		} catch (UnavailableException e) {
			c = null;
			e.printStackTrace();
		} catch (TimedOutException e) {
			c = null;
			e.printStackTrace();
		} catch (NotFoundException e) {
			c = null;
			e.printStackTrace();
		} catch (TException e) {
			c = null;
			e.printStackTrace();
		}
        
        long endTime = System.currentTimeMillis();
        log.info("Time to fetch the key "+key+":"+(endTime-startTime));
        
        return (c==null?null:new String(c.getValue()));
	}
}