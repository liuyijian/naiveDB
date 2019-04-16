package storage;


public class Test {
	
	public static void main(String[] args) {

		Node<Integer, String> myTree = new ExternalNode<Integer, String>(4, null, null);  
        int max = 10;  
        
        for(int i = 0; i < max; i++) {  
        	Node<Integer, String> r = myTree.insert(i, "No.1 of" + String.valueOf(i)); 
            myTree = r == null ? myTree : r;
            r = myTree.insert(i, "No.2 of" + String.valueOf(i)); 
            myTree = r == null ? myTree : r;
        }
                
        System.out.println(myTree.findBetween(0, true, max, true));
        System.out.println(myTree.findNotEqual(2, 0));
        System.out.println(myTree.findSmaller(5, true, 0));
        System.out.println(myTree.findLarger(5, true));
        System.out.println(myTree.findAll(2));

        System.out.println("----------------------------------");

        for(int i = 0; i < max; i ++) {  
        	Node<Integer, String> r = myTree.delete(i); 
            myTree = r == null ? myTree : r;
            System.out.println(myTree.findBetween(0, true, max, true));
        }
        
        System.out.println("----------------------------------");
        
        System.out.println(myTree.findBetween(0, true, max, true));
        System.out.println(myTree.findNotEqual(2, 0));
        System.out.println(myTree.findSmaller(5, true, 0));
        System.out.println(myTree.findLarger(5, true));
        System.out.println(myTree.findAll(2));
	}
}
