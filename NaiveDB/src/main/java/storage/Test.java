package storage;

import java.io.IOException;
import java.util.Vector;


public class Test {
	
	public static void bPlusTreeTest() {
		
    	BPlusTree<Integer, String> myTree = new BPlusTree<Integer, String>(10);  
        int MAX = 1000000;
        int count = 0;

        for(int i = MAX; i > -MAX; i--) {  
        	myTree.insert(i, "No." + String.valueOf(count++)); 
        }
        for(int i = MAX; i > -MAX; i--) {  
      	    myTree.insert(i, "No." + String.valueOf(count++)); 
      	    myTree.insert(i, "No." + String.valueOf(count++)); 
        }
        for(int i = MAX; i > -MAX; i--) {  
        	myTree.insert(i, "No." + String.valueOf(count++)); 
        }
      
        myTree.deleteBetween(-10, true, 10, false);
        System.out.println(myTree.findBetween(-20, true, 20, true));	
	}
	
	public static void storageWriteTest() throws IOException {
		
//		protected static final Integer TYPE_INT    = 3;
//		protected static final Integer TYPE_LONG   = 4;
//		protected static final Integer TYPE_FLOAT  = 5;
//		protected static final Integer TYPE_DOUBLE = 6;
//		protected static final Integer TYPE_STRING = 7;
		
		Vector<Integer> types = new Vector<Integer>();
		Vector<String> attrs = new Vector<String>();
		for (int i = 3; i < 8; ++i) {
			types.add(i);
			attrs.add("attr_" + (new Integer(i)).toString());
		}
		
		Vector<Integer> offsetsInRow = new Vector<Integer>();
		offsetsInRow.add(4);
		offsetsInRow.add(8);
		offsetsInRow.add(4);
		offsetsInRow.add(8);
		offsetsInRow.add(26);
		
		Storage test = new Storage(Storage.CONSTRUCT_FROM_NEW_DB, 
                                   "/Users/steve/github-repos/naiveDB/test.db", 
                                   types, attrs, offsetsInRow, 4, "attr_4");
		
		int MAX = 80;
        int count = 0;
		Vector<Vector<Object>> data = new Vector<Vector<Object>>();
		
		for(int i = MAX; i > -MAX; i--) {   
			Vector<Object> data1 = new Vector<Object>();
			data1.add(new Integer(i));
			data1.add(new Long(i));
			data1.add(new Float(i));
			data1.add(new Double(i));
			data1.add(new String((new Integer(i)).toString()));
			data.add(data1);
		}
		
        for(int i = 0; i < data.size(); i++) {  
      	    test.insert(data.get(i)); 
      	    test.insert(data.get(i)); 
        }
        
        test.deleteBetween(new Long(-10), true, new Long(10), false);
        
        data = new Vector<Vector<Object>>();
		
		for(int i = 5; i > -5; i--) {   
			Vector<Object> data1 = new Vector<Object>();
			data1.add(new Integer(i));
			data1.add(new Long(i));
			data1.add(new Float(i));
			data1.add(new Double(i));
			data1.add(new String((new Integer(i)).toString()));
			data.add(data1);
		}
		for(int i = 0; i < data.size(); i++) {  
      	    test.insert(data.get(i)); 
      	    test.insert(data.get(i)); 
        }
		
        System.out.println(test.index.findBetween(new Long(-15), true, new Long(15), 
        		                                  true));	
	}
	
	public static void storageReadTest() throws IOException {
		
//		protected static final Integer TYPE_INT    = 3;
//		protected static final Integer TYPE_LONG   = 4;
//		protected static final Integer TYPE_FLOAT  = 5;
//		protected static final Integer TYPE_DOUBLE = 6;
//		protected static final Integer TYPE_STRING = 7;
		
		Vector<Integer> types = new Vector<Integer>();
		Vector<String> attrs = new Vector<String>();
		for (int i = 3; i < 8; ++i) {
			types.add(i);
			attrs.add("attr_" + (new Integer(i)).toString());
		}
		
		Vector<Integer> offsetsInRow = new Vector<Integer>();
		offsetsInRow.add(4);
		offsetsInRow.add(8);
		offsetsInRow.add(4);
		offsetsInRow.add(8);
		offsetsInRow.add(26);
		
		Storage test = new Storage(Storage.CONSTRUCT_FROM_EXISTED_DB, 
                                   "/Users/steve/github-repos/naiveDB/test.db", 
                                   types, attrs, offsetsInRow, 4, "attr_4");
		
        System.out.println(test.index.findBetween(new Long(-15), true, new Long(15), 
        		                                  true));	
	}

	public static void main(String[] args) throws IOException {
		
		storageWriteTest();
		storageReadTest();
	}
}
