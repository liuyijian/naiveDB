package storage;

import java.util.Vector;


public class Test {
	public static void main(String[] args) {
		Node<Integer, String> myTree = new ExternalNode<Integer, String>(4, null, null);  
        
        int max = 200;  
        long start = System.currentTimeMillis();  
        for(int i = 0; i < max; i++) {  
        	Node<Integer, String> r = myTree.insert(i, String.valueOf(i)); 
            myTree = r == null ? myTree : r;
        }
        for(int i = 0; i < max; i += 2) {  
        	Node<Integer, String> r = myTree.delete(i); 
            myTree = r == null ? myTree : r;
        }
        Vector<String> resultSet = myTree.findAll(0, true, 100, true);
        System.out.println("time cost: " + (System.currentTimeMillis() - start));  
        System.out.println("succeeded");  
	}
}
