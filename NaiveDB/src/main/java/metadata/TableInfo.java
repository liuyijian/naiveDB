package metadata;

import java.util.Vector;

public class TableInfo {
    public Vector<Integer> types;
    public Vector<String> attrs;
    public Vector<Integer> offsets;
    public Vector<Integer> pktypes;
    public Vector<String> pkattrs;
    public Vector<Boolean> notnull;
    public String filepath;

    public TableInfo(){
        types = new Vector<Integer>();
        attrs = new Vector<String>();
        offsets = new Vector<Integer>();
        pktypes = new Vector<Integer>();
        pkattrs = new Vector<String>();
        notnull = new Vector<Boolean>();
    }
}
