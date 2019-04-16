package storage;


public class Test {
	
	public static void main(String[] args) {

		BPlusTree<Integer, String> myTree = new BPlusTree<Integer, String>(4);  
        int MAX = 200;
        int count = 0;

        for(int i = MAX; i > -MAX; i--) {  
        	myTree.insert(i, "No." + String.valueOf(count++)); 
        }
        for(int i = MAX; i > -MAX; i--) {  
        	myTree.insert(i, "No." + String.valueOf(count++)); 
        	myTree.insert(i, "No." + String.valueOf(count++)); 
        }
        
        myTree.deleteBetween(-5, true, 10, false);
        System.out.println(myTree.findBetween(-10, true, 20, true));
	}
}
