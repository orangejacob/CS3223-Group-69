// Lab 5: Aggregation Sum.
package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>sum</i> aggregation function.
 * @author Edward Sciore
 */
public class SumFn implements AggregationFn {
	private int sum;
	private String fldname;

	/**
	 * Create a sum aggregation function for the specified field.
	 * @param fldname the name of the aggregated field
	 */
	public SumFn(String fldname) {
		this.fldname = fldname;
	}

	/**
	 * Start a new sum.
	 * Since SimpleDB does not support null values,
	 * every record will be added,
	 * regardless of the field.
	 * The current sum is thus set to the first value.
	 * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
	 */
	public void processFirst(Scan s) {
		sum = s.getInt(fldname);
	}

	/**
	 * Since SimpleDB does not support null values,
	 * this method always increments the sum,
	 * regardless of the field.
	 * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
	 */
	public void processNext(Scan s) {
		sum += s.getInt(fldname);
	}

	/**
	 * Return the field's name, prepended by "sumof".
	 * @see simpledb.materialize.AggregationFn#fieldName()
	 */
	public String fieldName() {
		return "sumof" + fldname;
	}

	/**
	 * Return the current sum.
	 * @see simpledb.materialize.AggregationFn#value()
	 */
	public Constant value() {
		return new Constant(sum);
	}
	
	// Lab 6: Query Plan
	public String toString(){ 
		return "Sum (" +  fldname + ")";
	}  
}
