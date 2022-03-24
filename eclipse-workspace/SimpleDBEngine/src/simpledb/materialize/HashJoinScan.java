package simpledb.materialize;

import simpledb.query.*;
import java.util.*;


public class HashJoinScan implements Scan {
	private Scan s1, s2;
	private int curIndex;
	private String fldname1, fldname2;
	private int curPartition, totalPartitions;
	private List<String> s1Fields;
	private Map<String, Constant> curS1Record;
	private ArrayList<Map<String, Constant>> curArrayList;
	private Map<Constant, ArrayList<Map<String, Constant>>> hashTable;

	/**
	 * Create a hash join scan for the two underlying scans.
	 * @param s1 the LHS scan, used as the Hash Table reference.
	 * @param s2 the RHS scan
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 * @param totalPartitions the number of partition available
	 * @param s1Fields the fields of p1.
	 */
	public HashJoinScan(Scan s1, String fldname1, Scan s2, String fldname2, int totalPartitions, List<String> s1Fields) {
		this.s1 = s1;
		this.s2 = s2;
		this.curIndex = 0;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.s1Fields = s1Fields;
		this.totalPartitions = totalPartitions;
		beforeFirst();
	}

	/**
	 * Close the scan by closing the two underlying scans.
	 * @see simpledb.query.Scan#close()
	 */
	public void close() {
		s1.close();
		s2.close();
	}

	/**
	 * Set-up the next partition being used.
	 * @see simpledb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		curPartition = 0;
		useNextPartition();
	}

	/**
	 * Overview: 
	 * curArrayList is the set of records in a particular slot in
	 * HashTable. It is initialized when a s2 record matches a slot.
	 * s2 scan will only move forward, after finishing the curArrayList.
	 * Every s2 will be hashed against the fldname2, and matched against
	 * the HashTable. When there is a match, initialize curArrayList.
	 * When s2 reaches the end, move to next partition.
	 * When there is no more partition to scan, return false.
	 * @see simpledb.query.Scan#next()
	 */
	public boolean next() {
		// There are still records to be matched with current s2 scan.
		if (curArrayList != null && curIndex < curArrayList.size()) {
			curS1Record = curArrayList.get(curIndex++);
			return true;
		}
		// Move s2 scan when curArrayList is finished.
		while(s2.next()) {
			// Hash s2 field value and match against hash table.
			if(s2.getVal(fldname2).hashCode() % totalPartitions == curPartition) {
				if(hashTable.keySet().contains(s2.getVal(fldname2))) {
					// Return true, and initialize curArrayList.
					curIndex = 0;
					curArrayList = hashTable.get(s2.getVal(fldname2));
					curS1Record = curArrayList.get(curIndex++);
					return true;
				}
			}
		}
		// Dereference curArrayList, for next partition.
		curArrayList = null;
		if(!useNextPartition())
			// No more next partition -> return false.
			return false;
		// Go to next.
		return next();
	}

	/** 
	 * Return the integer value of the specified field.
	 * The value is obtained from whichever scan
	 * contains the field.
	 * @see simpledb.query.Scan#getInt(java.lang.String)
	 */
	public int getInt(String fldname) {
		if (s2.hasField(fldname))
			return s2.getInt(fldname);
		else
			return curS1Record.get(fldname).asInt();
	}

	/** 
	 * Return the string value of the specified field.
	 * The value is obtained from whichever scan
	 * contains the field.
	 * @see simpledb.query.Scan#getString(java.lang.String)
	 */
	public String getString(String fldname) {
		if (s2.hasField(fldname))
			return s2.getString(fldname);
		else
			return curS1Record.get(fldname).asString();
	}

	/** 
	 * Return the value of the specified field.
	 * The value is obtained from whichever scan
	 * contains the field.
	 * @see simpledb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		if (s2.hasField(fldname))
			return s2.getVal(fldname);
		else
			return curS1Record.get(fldname);
	}

	/**
	 * Return true if the specified field is inouterPages
	 * either of the underlying scans.
	 * @see simpledb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		return s2.hasField(fldname) || curS1Record.containsKey(fldname);
	}

	private boolean useNextPartition() {
		// No more partition, returns false.
		if(++curPartition >= totalPartitions)
			return false;
		// Place s1 and s2 scan before the first record.
		s1.beforeFirst();
		s2.beforeFirst();
		hashTable = new HashMap<>();
		// Traverse s1 Scan to setup hash table.
		while(s1.next()) {
			if(s1.getVal(fldname1).hashCode() % totalPartitions == curPartition) {
				// Store the partitioned record into HashMap's ArrayList as HashMap with reference to field name and value.
				Map<String, Constant> record = new HashMap<>();
				for(String fieldName: s1Fields) 
					record.put(fieldName, s1.getVal(fieldName));
				hashTable.computeIfAbsent(s1.getVal(fldname1), k -> new ArrayList<Map<String, Constant>>()).add(record);
			}
		}
		
		return true;

	}
}

