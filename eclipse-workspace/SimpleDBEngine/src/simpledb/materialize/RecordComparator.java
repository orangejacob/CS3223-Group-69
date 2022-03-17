package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * A comparator for scans.
 * @author Edward Sciore
 */
public class RecordComparator implements Comparator<Scan> {
	private List<String> fields = null;
	private LinkedHashMap<String, Integer> sortFields;
	/**
	 * Create a comparator using the specified fields,
	 * using the ordering implied by its iterator.
	 * @param fields a list of field names
	 */


	public RecordComparator(List<String> fields) {
		this.fields = fields;
	}

	/* 
	 * Lab 3: Order By
	 * Overload constructor to take in LinkedHashMap
	 * attribute which keep tracks of the order to 
	 * sort and either ascending or descending.
	 */
	public RecordComparator(LinkedHashMap<String, Integer> sortFields) {
		this.sortFields = sortFields;
	}

	/**
	 * Compare the current records of the two specified scans.
	 * The sort fields are considered in turn.
	 * When a field is encountered for which the records have
	 * different values, those values are used as the result
	 * of the comparison.
	 * If the two records have the same values for all
	 * sort fields, then the method returns 0.
	 * @param s1 the first scan
	 * @param s2 the second scan
	 * @return the result of comparing each scan's current record according to the field list
	 */

	/* 
	 * Lab 3: Order By
	 * Overload constructor to take in LinkedHashMap
	 * attribute which keep tracks of the order to 
	 * sort and either ascending or descending.
	 * 
	 * Lab 6: Distinct
	 * For Distinct's default sorting, we use
	 * fields instead of sortFields to handle
	 * sorting.
	 */

	public int compare(Scan s1, Scan s2) {
		// Lab 6
		if (fields != null) {
			for (String fldname : fields) {
				Constant val1 = s1.getVal(fldname);
				Constant val2 = s2.getVal(fldname);
				int result = val1.compareTo(val2);
				if (result != 0)
					return result;
			}
			return 0;
		}
		// Lab 3: Order By
		for(Map.Entry<String, Integer> entry: sortFields.entrySet()) {
			// Value 1 = Ascending, -1 = Descending
			Constant val1 = s1.getVal(entry.getKey());
			Constant val2 = s2.getVal(entry.getKey());
			int result = val1.compareTo(val2);
			if (result != 0)
				return result * entry.getValue();
		}
		return 0;
	}

	public boolean compareDistinct(Scan s1, Scan s2) {
		for (String fldname : fields) {
			Constant val1 = s1.getVal(fldname);
			Constant val2 = s2.getVal(fldname);
			int result = val1.compareTo(val2);
			if (result == 0)
				return false;
		}
		return true;
	}
}
