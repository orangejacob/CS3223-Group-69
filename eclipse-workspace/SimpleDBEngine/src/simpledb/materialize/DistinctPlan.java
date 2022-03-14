package simpledb.materialize;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
public class DistinctPlan implements Plan {
	private Transaction tx;
	private Plan p;
	private Schema sch;
	private RecordComparator comp;
	private List<String> distinctFields;
	private Map<String, Map<Constant, Boolean>> hashTable;

	/**
	 * Create a sort plan for the specified query.
	 * @param tx the calling transaction
	 * @param p the plan for the underlying query
	 * @param sortfields the fields to sort by
	 */

	// Lab 6: SortFields for the fields that have Distinct selection.
	public DistinctPlan(Transaction tx, Plan p, List<String> distinctFields) {
		this.tx = tx;
		this.p = p;
		sch = p.schema();
		this.distinctFields = distinctFields;
		this.hashTable = new HashMap<String, Map<Constant, Boolean>>();
		comp = new RecordComparator(distinctFields);
	}

	/**
	 * This method is where most of the action is.
	 * Up to 2 sorted temporary tables are created,
	 * and are passed into SortScan for final merging.
	 * @see simpledb.plan.Plan#open()
	 */
	public Scan open() {
		Scan src = p.open();
		List<TempTable> runs = splitIntoRuns(src);
		src.close();
		// There's only 1 run -> so just remove duplicates.
		if(runs.size() == 1) 
			runs.add(removeDuplicates(runs.get(0)));
		while (runs.size() > 1) 
			runs = doAMergeIteration(runs);
		return runs.get(0).open();
	}

	/**
	 * Return the number of blocks in the sorted table,
	 * which is the same as it would be in a
	 * materialized table.
	 * It does <i>not</i> include the one-time cost
	 * of materializing and sorting the records.
	 * @see simpledb.plan.Plan#blocksAccessed()
	 */
	public int blocksAccessed() {
		// does not include the one-time cost of sorting
		Plan mp = new MaterializePlan(tx, p); // not opened; just for analysis
		return mp.blocksAccessed();
	}

	/**
	 * Return the number of records in the sorted table,
	 * which is the same as in the underlying query.
	 * @see simpledb.plan.Plan#recordsOutput()
	 */
	public int recordsOutput() {
		return p.recordsOutput();
	}

	/**
	 * Return the number of distinct field values in
	 * the sorted table, which is the same as in
	 * the underlying query.
	 * @see simpledb.plan.Plan#distinctValues(java.lang.String)
	 */
	public int distinctValues(String fldname) {
		return p.distinctValues(fldname);
	}

	/**
	 * Return the schema of the sorted table, which
	 * is the same as in the underlying query.
	 * @see simpledb.plan.Plan#schema()
	 */
	public Schema schema() {
		return sch;
	}

	private List<TempTable> splitIntoRuns(Scan src) {
		List<TempTable> temps = new ArrayList<>();
		src.beforeFirst();
		if (!src.next())
			return temps;
		TempTable currenttemp = new TempTable(tx, sch);
		temps.add(currenttemp);
		UpdateScan currentscan = currenttemp.open();
		while (copy(src, currentscan))
			if (comp.compare(src, currentscan) < 0) {
				// start a new run
				currentscan.close();
				currenttemp = new TempTable(tx, sch);
				temps.add(currenttemp);
				currentscan = (UpdateScan) currenttemp.open();
			}
		currentscan.close();
		return temps;
	}

	private List<TempTable> doAMergeIteration(List<TempTable> runs) {
		List<TempTable> result = new ArrayList<>();
		while (runs.size() > 1) {
			TempTable p1 = runs.remove(0);
			TempTable p2 = runs.remove(0);
			result.add(mergeTwoRuns(p1, p2));
		}
		if (runs.size() == 1) {
			result.add(runs.get(0));
		}
		return result;
	}

	// Lab 6: Merge + Remove duplicates.
	private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
		Scan src1 = p1.open();
		Scan src2 = p2.open();
		TempTable result = new TempTable(tx, sch);
		UpdateScan dest = result.open();
		boolean hasmore1 = src1.next();
		boolean hasmore2 = src2.next();
		boolean nexted1, nexted2;
		// Initialize the hash table, in order to keep track duplicates.
		for (String fieldName: this.distinctFields)
			hashTable.put(fieldName, new HashMap<Constant, Boolean>());
		Map<Constant, Boolean> hashMap = null;
		while (hasmore1 && hasmore2) {
			// Check if any is duplicated
			nexted1 = nexted2 = false;
			for(String fieldName: distinctFields) {
				// Get the hash map
				hashMap = hashTable.get(fieldName);
				Constant val1 = src1.getVal(fieldName);
				Constant val2 = src2.getVal(fieldName);
				if(hashMap.containsKey(val1))
					nexted1 = true;
				if(hashMap.containsKey(val2))
					nexted2 = true;
			}
			// Both doesn't contain duplication fields based on existing record.
			if(!nexted1 && !nexted2) {
				boolean allDistinctValue = comp.compareDistinct(src1, src2);
				if(comp.compare(src1, src2) < 0) {
					stashRecordValue(src1);
					hasmore1 = copy(src1, dest);
					if(!allDistinctValue)
						hasmore2 = src2.next();
				}else {
					stashRecordValue(src2);
					hasmore2 = copy(src2, dest);
					if(!allDistinctValue)
						hasmore1 = src1.next();
				}
			}else if(!nexted1) {
				stashRecordValue(src1);
				hasmore1 = copy(src1, dest);
				hasmore2 = src2.next();
			}else if(!nexted2) {
				stashRecordValue(src2);
				hasmore2 = copy(src2, dest);
				hasmore1 = src1.next();
			}else {
				hasmore1 = src1.next();
				hasmore2 = src2.next();
			}
		}

		if (hasmore1)
			while (hasmore1) {
				boolean nexted = false;
				for(String fieldName: distinctFields) {
					// Get the hash map
					hashMap = hashTable.get(fieldName);
					Constant val = src1.getVal(fieldName);
					if(hashMap.containsKey(val)) {
						nexted = true;
						hasmore1 = src1.next();
						break;
					}
				}
				if(!nexted) {
					stashRecordValue(src1);
					hasmore1 = copy(src1, dest);
				}
			}
		else
			while (hasmore2) {				
				boolean nexted = false;
				for(String fieldName: distinctFields) {
					// Get the hash map
					hashMap = hashTable.get(fieldName);
					Constant val = src2.getVal(fieldName);
					if(hashMap.containsKey(val)) {
						nexted = true;
						hasmore2 = src2.next();
						break;
					}
				}
				if(!nexted) {
					stashRecordValue(src1);
					hasmore2 = copy(src2, dest);
				}
			}
		src1.close();
		src2.close();
		dest.close();
		return result;
	}

	private TempTable removeDuplicates(TempTable p) {
		// Initialize the hash table, in order to keep track duplicates.
		for (String fieldName: this.distinctFields) {
			hashTable.put(fieldName, new HashMap<Constant, Boolean>());
		}
		Scan src = p.open();
		TempTable result = new TempTable(tx, sch);
		UpdateScan dest = result.open();
		boolean hasmore = src.next(), nexted = false;
		Map<Constant, Boolean> hashMap = null;

		while (hasmore) {
			// Check if any is duplicated
			for(String fieldName: distinctFields) {
				// Get the hash map
				hashMap = hashTable.get(fieldName);
				Constant val = src.getVal(fieldName);
				if(hashMap.containsKey(val)) {
					nexted = true;
					break;
				}
			}
			if(!nexted) {
				stashRecordValue(src);
				hasmore = copy(src, dest);
			}else {
				hasmore = src.next();
			}
		}
		src.close();
		dest.close();
		return result;
	}

	private boolean copy(Scan src, UpdateScan dest) {
		dest.insert();
		for (String fldname : sch.fields())
			dest.setVal(fldname, src.getVal(fldname));
		return src.next();
	}

	private void stashRecordValue(Scan s) {
		Map<Constant, Boolean> hashMap = null;
		for(String fieldName: distinctFields) {
			// Get the hash map
			hashMap = hashTable.get(fieldName);
			Constant val = s.getVal(fieldName);
			hashMap.put(val, true);
		}
	}
}
