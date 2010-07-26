package utils;

import java.io.IOException;
import java.io.Reader;

import com.hp.hpl.jena.shared.JenaException;

public class IStream {

    // simple input stream handler

    Reader in;
    char[] thisChar = new char[1];
    boolean eof;
    int charpos = 1;
    int linepos = 1;

    public IStream(Reader in) {
        try {
            this.in = in;
            eof = (in.read(thisChar, 0, 1) == -1);
        } catch (IOException e) {
        	e.printStackTrace();
            throw new JenaException(e);
        }
    }

    public char readChar() {
        try {
            if (eof)
                return '\000';
            char rv = thisChar[0];
            eof = (in.read(thisChar, 0, 1) == -1);
            if (rv == '\n') {
                linepos++;
                charpos = 0;
            } else {
                charpos++;
            }
            return rv;
        } catch (java.io.IOException e) {
            throw new JenaException(e);
        }
    }

    public char nextChar() {
        return eof ? '\000' : thisChar[0];
    }

    public boolean eof() {
        return eof;
    }

    public int getLinepos() {
        return linepos;
    }

    public int getCharpos() {
        return charpos;
    }
    
    public void close() throws IOException{
    	in.close();
    }
    
    
}