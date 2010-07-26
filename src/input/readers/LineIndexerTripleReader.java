package input.readers;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Hashtable;

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

public class LineIndexerTripleReader extends BaseRecordReader<Text,Triple>{
	
	private static final String EMPTY = "";
	
	private int recordCounter = 0;
	private boolean inErr = false;
	private int errCount = 0;
	private static final int sbLength = 200;
	
	private Hashtable<String, Resource> anons = new Hashtable<String, Resource>();

	private static Logger log = LoggerFactory.getLogger(LineIndexerTripleReader.class);
	
	private RDFErrorHandler errorHandler = new RDFDefaultErrorHandler();
	
	/**
     * Already with ": " at end for error messages.
     */
    private String base;
	
	public LineIndexerTripleReader(Configuration job, MultiFileSplit input) throws IOException {
		super(job, input);		
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public Triple createValue() {
		System.out.println("selfdebug: create Triple value");
		Triple returnable = new Triple(Node.createAnon(),Node.createAnon(),Node.createAnon());
		System.out.println("selfdebug: Triple created");
		return returnable;
	}

	@Override
	public boolean next(Text key, Triple value) throws IOException {
		
		String line = null;
		do {
			if (in != null) {				
				if(!in.eof()){
					if(populateKeyValue(key,value)){
						return true;	
					}
				}
			}
			
			if (!openNextFile()){
					System.out.println("openNextFile() returned False");
					return false;
			}			
			
		} while (!in.eof());
		return false;
	}
	
	protected boolean populateKeyValue(Text key, Triple value){

        Resource subject;
        Property predicate = null;
        RDFNode object;
		
		inErr = false;

        skipWhiteSpace();
        if (in.eof()) {
            return false;
        }

        subject = readResource();                
        if (inErr)
            return false;

        skipWhiteSpace();
        try {
            predicate = model.createProperty(readResource().getURI());                    
        } catch (Exception e1) {
        	e1.printStackTrace();
            errorHandler.fatalError(e1);                            
        }
        if (inErr)
            return false;

        skipWhiteSpace();
        object = readNode();                
        if (inErr)
            return false;

        skipWhiteSpace();
        if (badEOF())
            return false;

        if (!expect("."))
            return false;

        try {
        	key.set(getCurrentFileName());
            value.setTriple(subject.asNode(), predicate.asNode(), object.asNode());
            recordCounter++;
            //System.out.println("Triple value:"+value);
            return true;
        } catch (Exception e2) {
        	e2.printStackTrace();
            errorHandler.fatalError(e2);                    
        }
        
        return false;
	}
	
	protected void skipWhiteSpace() {
        while (Character.isWhitespace(in.nextChar()) || in.nextChar() == '#') {
            char inChar = in.readChar();
            if (in.eof()) {
                return;
            }
            if (inChar == '#') {
                while (inChar != '\n') {
                    inChar = in.readChar();
                    if (in.eof()) {
                        return;
                    }
                }
            }
        }
    }
	
	public Resource readResource()  {
        char inChar = in.readChar();
        if (badEOF())
            return null;

        if (inChar == '_') { // anon resource
            if (!expect(":"))
                return null;
            String name = readName();
            if (name == null) {
                syntaxError("expected bNode label");
                return null;
            }
            return lookupResource(name);
        } else if (inChar == '<') { // uri
            String uri = readURI();
            if (uri == null) {
                inErr = true;
                return null;
            }
            inChar = in.readChar();
            if (inChar != '>') {
                syntaxError("expected '>'");
                return null;
            }
            return model.createResource(uri);
        } else {
            syntaxError("unexpected input");
            return null;
        }
    }

    public RDFNode readNode()  {
        skipWhiteSpace();
        switch (in.nextChar()) {
            case '"' :
                return readLiteral(false);
            case 'x' :
                return readLiteral(true);
            case '<' :
            case '_' :
                return readResource();
            default :
                syntaxError("unexpected input");
                return null;
        }
    }

    protected Literal readLiteral(boolean wellFormed)  {

        StringBuffer lit = new StringBuffer(sbLength);

        if (wellFormed) {
            deprecated("Use ^^rdf:XMLLiteral not xml\"literals\", .");

            if (!expect("xml"))
                return null;
        }

        if (!expect("\""))
            return null;

        while (true) {
            char inChar = in.readChar();
            if (badEOF())
                return null;
            if (inChar == '\\') {
                char c = in.readChar();
                if (in.eof()) {
                    inErr = true;
                    return null;
                }
                if (c == 'n') {
                    inChar = '\n';
                } else if (c == 'r') {
                    inChar = '\r';
                } else if (c == 't') {
                    inChar = '\t';
                } else if (c == '\\' || c == '"') {
                    inChar = c;
                } else if (c == 'u') {
                    inChar = readUnicode4Escape();
                    if (inErr)
                        return null;
                } else {
                    syntaxError("illegal escape sequence '" + c + "'");
                    return null;
                }
            } else if (inChar == '"') {
                String lang;
                if ('@' == in.nextChar()) {
                    expect("@");
                   lang = readLang();
                } else if ('-' == in.nextChar()) {
                    expect("-");
                    deprecated("Language tags should be introduced with @ not -.");
                    lang = readLang();
                } else {
                    lang = "";
                }
                if (wellFormed) {
                    return model.createLiteral(
                        lit.toString(),
//                        "",
                        wellFormed);
                } else if ('^' == in.nextChar()) {
                    String datatypeURI = null;
                    if (!expect("^^<")) {
                        syntaxError("ill-formed datatype");
                        return null;
                    }
                    datatypeURI = readURI();
                    if (datatypeURI == null || !expect(">"))
                        return null;
					if ( lang.length() > 0 )
					   deprecated("Language tags are not permitted on typed literals.");
                    
                    return model.createTypedLiteral(
                        lit.toString(),
                        datatypeURI);
                } else {
                    return model.createLiteral(lit.toString(), lang);
                }
            }
            lit = lit.append(inChar);
        }
    }
    

    private char readUnicode4Escape() {
        char buf[] =
            new char[] {
                in.readChar(),
                in.readChar(),
                in.readChar(),
                in.readChar()};
        if (badEOF()) {
            return 0;
        }
        try {
            return (char) Integer.parseInt(new String(buf), 16);
        } catch (NumberFormatException e) {
            syntaxError("bad unicode escape sequence");
            return 0;
        }
    }
    private void deprecated(String s) {
        errorHandler.warning(
            new SyntaxError(
                syntaxErrorMessage(
                    "Deprecation warning",
                    s,
                    in.getLinepos(),
                    in.getCharpos())));
    }

    private void syntaxError(String s) {
        errorHandler.error(
            new SyntaxError(
                syntaxErrorMessage(
                    "Syntax error",
                    s,
                    in.getLinepos(),
                    in.getCharpos())));
        inErr = true;
    }
    private String readLang() {
        StringBuffer lang = new StringBuffer(15);


        while (true) {
            char inChar = in.nextChar();
            if (Character.isWhitespace(inChar) || inChar == '.' || inChar == '^')
                return lang.toString();
            lang = lang.append(in.readChar());
        }
    }
    private boolean badEOF() {
        if (in.eof()) {
            syntaxError("premature end of file");
        }
        return inErr;
    }
    protected String readURI() {
        StringBuffer uri = new StringBuffer(sbLength);

        while (in.nextChar() != '>') {
            char inChar = in.readChar();

            if (inChar == '\\') {
                expect("u");
                inChar = readUnicode4Escape();
            }
            if (badEOF()) {
                return null;
            }
            uri = uri.append(inChar);
        }
        return uri.toString();
    }

    protected String readName() {
        StringBuffer name = new StringBuffer(sbLength);

        char nextChar;
        while (Character.isLetterOrDigit(nextChar=in.nextChar())
        		|| '-'==nextChar ) {
            name = name.append(in.readChar());
            if (badEOF())
                return null;
        }
        return name.toString();
    }
    private boolean expect(String str) {
        for (int i = 0; i < str.length(); i++) {
            char want = str.charAt(i);

            if (badEOF())
                return false;

            char inChar = in.readChar();

            if (inChar != want) {
                //System.err.println("N-triple reader error");
                syntaxError("expected \"" + str + "\"");
                return false;
            }
        }
        return true;
    }    

    protected Resource lookupResource(String name)  {
        Resource r;
        r = anons.get(name);
        if (r == null) {
            r = model.createResource();
            anons.put(name, r);
        }
        return r;
    }

    protected String syntaxErrorMessage(
        String sort,
        String msg,
        int linepos,
        int charpos) {
        return base
            + sort
            + " at line "
            + linepos
            + " position "
            + charpos
            + ": "
            + msg;
    }

}
