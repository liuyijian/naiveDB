package storage;

import java.io.File;
import java.io.IOException;
import java.util.Vector;


public class Test {
	
    public static boolean deleteFile(String fileName) {
        File file = new File(fileName);
        // 如果文件路径所对应的文件存在，并且是一个文件，则直接删除
	    if (file.exists() && file.isFile()) {
	        if (file.delete()) {
	            System.out.println("删除单个文件" + fileName + "成功！");
	            return true;
	        } else {
	            System.out.println("删除单个文件" + fileName + "失败！");
	            return false;
	        }
	    } else {
	        System.out.println("删除单个文件失败：" + fileName + "不存在！");
	        return false;
	    }
	}
	
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
	
    public static void storageWriteTestWithSinglePk() throws IOException {
				
		deleteFile("/Users/steve/github-repos/naiveDB/test.db");
		// 构造DB
		
		Vector<Integer> types = new Vector<Integer>();
		Vector<String>  attrs = new Vector<String>();
		for (int i = 3; i < 8; ++i) {
			types.add(i);
			attrs.add("attr_" + (new Integer(i)).toString());
		}
		
		Vector<Integer> pkTypes = new Vector<Integer>();
		pkTypes.add(types.get(0));

		Vector<String>  pkAttrs = new Vector<String>();
		pkAttrs.add(attrs.get(0));
		
		Vector<Integer> pkIndexes = new Vector<Integer>();
		pkIndexes.add(0);
		
		Vector<Integer> offsetsInRow = new Vector<Integer>();
		offsetsInRow.add(Type.OFFSET_INT);
		offsetsInRow.add(Type.OFFSET_LONG);
		offsetsInRow.add(Type.OFFSET_FLOAT);
		offsetsInRow.add(Type.OFFSET_DOUBLE);
		offsetsInRow.add(40 * 2);
		
		Vector<Boolean> notNull = new Vector<Boolean>();
		for (int i = 0; i < 5; ++i) {
			notNull.add(true);
		}
		
		Storage test = new Storage(Storage.CONSTRUCT_FROM_NEW_DB, "",
                                   "/Users/steve/github-repos/naiveDB/test.db", 
                                   types, attrs, pkTypes, pkAttrs, offsetsInRow, notNull);
		
		// 生成数据
		 
		int MAX = 30;
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
		
		// 测试插入
		
        for(int i = 0; i < data.size(); i++) {  
      	    test.insert(data.get(i)); 
      	    test.insert(data.get(i)); 
        }
        
        // 测试删除
        Vector<Object> left = new Vector<Object>();
        left.add(-10);
        PrimaryKey leftPk = new PrimaryKey(pkTypes, left, pkIndexes);
        
        Vector<Object> right = new Vector<Object>();
        right.add(10);
        PrimaryKey rightPk = new PrimaryKey(pkTypes, right, pkIndexes);
        
        test.deleteBetweenPk(leftPk, true, rightPk, false);
        
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
		
		left = new Vector<Object>();
        left.add(-15);
        leftPk = new PrimaryKey(pkTypes, left, pkIndexes);
        
        right = new Vector<Object>();
        right.add(15);
        rightPk = new PrimaryKey(pkTypes, right, pkIndexes);
        
        System.out.println(test.index.findBetween(leftPk, true, rightPk, true));	
        
        test.save();
	}
	
	public static void storageReadTestWithSinglePk() throws IOException {
				
		// 构造DB
		Vector<Integer> types = new Vector<Integer>();
		Vector<String>  attrs = new Vector<String>();
		for (int i = 3; i < 8; ++i) {
			types.add(i);
			attrs.add("attr_" + (new Integer(i)).toString());
		}
		
		Vector<Integer> pkTypes = new Vector<Integer>();
		pkTypes.add(types.get(0));

		Vector<String>  pkAttrs = new Vector<String>();
		pkAttrs.add(attrs.get(0));
		
		Vector<Integer> pkIndexes = new Vector<Integer>();
		pkIndexes.add(0);
		
		Vector<Integer> offsetsInRow = new Vector<Integer>();
		offsetsInRow.add(Type.OFFSET_INT);
		offsetsInRow.add(Type.OFFSET_LONG);
		offsetsInRow.add(Type.OFFSET_FLOAT);
		offsetsInRow.add(Type.OFFSET_DOUBLE);
		offsetsInRow.add(40 * 2);
		
		Vector<Boolean> notNull = new Vector<Boolean>();
		for (int i = 0; i < 5; ++i) {
			notNull.add(true);
		}
		
		Storage test = new Storage(Storage.CONSTRUCT_FROM_EXISTED_DB, "",
                                   "/Users/steve/github-repos/naiveDB/test.db", 
                                   types, attrs, pkTypes, pkAttrs, offsetsInRow, notNull);

		// 测试
		Vector<Object> left = new Vector<Object>();
        left.add(-15);
        PrimaryKey leftPk = new PrimaryKey(pkTypes, left, pkIndexes);
        
        Vector<Object> right = new Vector<Object>();
        right.add(15);
        PrimaryKey rightPk = new PrimaryKey(pkTypes, right, pkIndexes);
		
        System.out.println(test.index.findBetween(leftPk, true, rightPk, true));	
	}

	public static void storageWriteTestWithMultiPks() throws IOException {
		
		deleteFile("/Users/steve/github-repos/naiveDB/test.db");
		// 构造DB
		
		Vector<Integer> types = new Vector<Integer>();
		Vector<String>  attrs = new Vector<String>();
		for (int i = 3; i < 8; ++i) {
			types.add(i);
			attrs.add("attr_" + (new Integer(i)).toString());
		}
		
		Vector<Integer> pkTypes = new Vector<Integer>();
		pkTypes.add(types.get(0));
		pkTypes.add(types.get(2));

		Vector<String>  pkAttrs = new Vector<String>();
		pkAttrs.add(attrs.get(0));
		pkAttrs.add(attrs.get(2));

		Vector<Integer> pkIndexes = new Vector<Integer>();
		pkIndexes.add(0);
		pkIndexes.add(2);
		
		Vector<Integer> offsetsInRow = new Vector<Integer>();
		offsetsInRow.add(Type.OFFSET_INT);
		offsetsInRow.add(Type.OFFSET_LONG);
		offsetsInRow.add(Type.OFFSET_FLOAT);
		offsetsInRow.add(Type.OFFSET_DOUBLE);
		offsetsInRow.add(40 * 2);
		
		Vector<Boolean> notNull = new Vector<Boolean>();
		for (int i = 0; i < 5; ++i) {
			notNull.add(true);
		}
		
		Storage test = new Storage(Storage.CONSTRUCT_FROM_NEW_DB, "",
                                   "/Users/steve/github-repos/naiveDB/test.db", 
                                   types, attrs, pkTypes, pkAttrs, offsetsInRow, notNull);
		
		// 生成数据
		 
		int MAX = 30;
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
		
		// 测试插入
		
        for(int i = 0; i < data.size(); i++) {  
      	    test.insert(data.get(i)); 
      	    test.insert(data.get(i)); 
        }
        
        // 测试删除
        Vector<Object> left = new Vector<Object>();
        left.add(new Integer(-10));
        left.add(new Long(-10));
        left.add(new Float(-10));
        left.add(new Double(-10));
        left.add(new String("-10"));
        PrimaryKey leftPk = new PrimaryKey(pkTypes, left, pkIndexes);
        
        Vector<Object> right = new Vector<Object>();
        right.add(new Integer(10));
        right.add(new Long(10));
        right.add(new Float(10));
        right.add(new Double(10));
        right.add(new String("10"));
        PrimaryKey rightPk = new PrimaryKey(pkTypes, right, pkIndexes);
        
        test.deleteBetweenPk(leftPk, true, rightPk, false);
        
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
		
		left = new Vector<Object>();
		left.add(new Integer(-15));
        left.add(new Long(-15));
        left.add(new Float(-15));
        left.add(new Double(-15));
        left.add(new String("-15"));
        leftPk = new PrimaryKey(pkTypes, left, pkIndexes);
        
        right = new Vector<Object>();
        right.add(new Integer(15));
        right.add(new Long(15));
        right.add(new Float(15));
        right.add(new Double(15));
        right.add(new String("15"));
        rightPk = new PrimaryKey(pkTypes, right, pkIndexes);
        
        System.out.println(test.index.findBetween(leftPk, true, rightPk, true));	
        
        test.save();
	}
	
	public static void storageReadTestWithMultiPks() throws IOException {
		
		// 构造DB
		Vector<Integer> types = new Vector<Integer>();
		Vector<String>  attrs = new Vector<String>();
		for (int i = 3; i < 8; ++i) {
			types.add(i);
			attrs.add("attr_" + (new Integer(i)).toString());
		}
		
		Vector<Integer> pkTypes = new Vector<Integer>();
		pkTypes.add(types.get(0));
		pkTypes.add(types.get(2));

		Vector<String>  pkAttrs = new Vector<String>();
		pkAttrs.add(attrs.get(0));
		pkAttrs.add(attrs.get(2));

		Vector<Integer> pkIndexes = new Vector<Integer>();
		pkIndexes.add(0);
		pkIndexes.add(2);
		
		Vector<Integer> offsetsInRow = new Vector<Integer>();
		offsetsInRow.add(Type.OFFSET_INT);
		offsetsInRow.add(Type.OFFSET_LONG);
		offsetsInRow.add(Type.OFFSET_FLOAT);
		offsetsInRow.add(Type.OFFSET_DOUBLE);
		offsetsInRow.add(40 * 2);
		
		Vector<Boolean> notNull = new Vector<Boolean>();
		for (int i = 0; i < 5; ++i) {
			notNull.add(true);
		}
		
		Storage test = new Storage(Storage.CONSTRUCT_FROM_EXISTED_DB, "",
                                   "/Users/steve/github-repos/naiveDB/test.db", 
                                   types, attrs, pkTypes, pkAttrs, offsetsInRow, notNull);

		// 测试
		Vector<Object> left = new Vector<Object>();
		left.add(new Integer(-15));
        left.add(new Long(-15));
        left.add(new Float(-15));
        left.add(new Double(-15));
        left.add(new String("-15"));
        PrimaryKey leftPk = new PrimaryKey(pkTypes, left, pkIndexes);
        
        Vector<Object> right = new Vector<Object>();
        right.add(new Integer(15));
        right.add(new Long(15));
        right.add(new Float(15));
        right.add(new Double(15));
        right.add(new String("15"));
        PrimaryKey rightPk = new PrimaryKey(pkTypes, right, pkIndexes);
		
        System.out.println(test.index.findBetween(leftPk, true, rightPk, true));	
	}
	
	public static void main(String[] args) throws IOException {
		
		storageWriteTestWithSinglePk();
		storageReadTestWithSinglePk();
		
		storageWriteTestWithMultiPks();
		storageReadTestWithMultiPks();
		
		Integer a = 1;
		Object c = a;
		Long b = (long) 1;
		Object d = b;
		System.out.println(c.toString());
	}
}
