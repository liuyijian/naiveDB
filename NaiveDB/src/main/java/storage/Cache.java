package storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;


public class Cache {
	
	protected Storage storage;
	protected Integer limitSize;
	protected LinkedList<Integer> cache; // the first element is the oldest one
	protected HashMap<Integer, Integer> count;
	
	public Cache(Storage storage, Integer cacheSize) {
		
		this.storage   = storage;
		this.limitSize = cacheSize / storage.offset;
		this.cache     = new LinkedList<Integer>();
		this.count     = new HashMap<Integer, Integer>();
	}
	
	public void put(Row row) throws IOException {
		
		Integer orderOfRow = row.order;
		Integer countOfRow = this.count.get(orderOfRow);
		
		if (countOfRow == null) {

			while (this.count.size() == this.limitSize) {
				
				Integer orderOfRowInCache = this.cache.getFirst();
				Integer countOfRowInCache = this.count.get(orderOfRowInCache);
				
				if (countOfRowInCache.equals(1)) {
				
					this.storage.data.get(orderOfRowInCache).release();
					
					this.cache.removeFirst();
					this.count.remove(orderOfRowInCache);
				}
				else {
					
					this.cache.addLast(this.cache.removeFirst());
					this.count.replace(orderOfRowInCache, countOfRowInCache - 1); 
				}
			}	
			
			this.cache.addLast(orderOfRow);
			this.count.put(orderOfRow, 1); 
		}
		else {
			this.count.replace(orderOfRow, countOfRow + 1); 
		}
	}
}