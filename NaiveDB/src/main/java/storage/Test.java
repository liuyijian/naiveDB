package storage;


public class Test {
	
	public static void main(String[] args) {

		BPlusTree<Integer, String> myTree = new BPlusTree<Integer, String>(4);  
        int MAX = 2;
        int count = 0;
        

        System.out.println("----------------------------------");
        for(int i = 0; i < MAX; i++) {  
        	myTree.insert(i, "No." + String.valueOf(count++)); 
            System.out.println(myTree.findBetween(0, true, MAX, true));
            System.out.println("----------------------------------");
        }
        
        System.out.println("");
        
        System.out.println("----------------------------------");
        for(int i = 0; i < MAX; i++) {  
        	myTree.insert(i, "No." + String.valueOf(count++)); 
            System.out.println(myTree.findBetween(0, true, MAX, true));
        	myTree.insert(i, "No." + String.valueOf(count++)); 
            System.out.println(myTree.findBetween(0, true, MAX, true));
            System.out.println("----------------------------------");
        }

        
        System.out.println("");
        System.out.println("----------------------------------");
        myTree.deleteBetween(0, true, 80, false);
        System.out.println(myTree.findBetween(0, true, MAX, true));
	}
}
