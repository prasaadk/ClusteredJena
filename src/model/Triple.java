package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

public class Triple extends com.hp.hpl.jena.graph.Triple {
	
	public Triple(Node s, Node p, Node o) {
		super(s, p, o);	 
	}

	public void setTriple(Node subject, Node predicate, Node object) {
		subj =  subject;
		pred =  predicate;
		obj = object;		
	}
}
