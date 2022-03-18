// ADDED
package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>min</i> aggregation function.
 * 
 * @author Edward Sciore
 */
public class MinFn implements AggregationFn {
	private Constant min;
    private String fldname;

    /**
     * Create a min aggregation function for the specified field.
     * 
     * @param fldname the name of the aggregated field
     */
    public MinFn(String fldname) {
        this.fldname = fldname;
    }

    /**
     * Start a new min.
     * Since SimpleDB does not support null values,
     * every record will be checked,
     * regardless of the field.
     * The current min is thus set to the first value.
     * 
     * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
     */
    public void processFirst(Scan s) {
        min = s.getVal(fldname);
    }

    /**
     * Since SimpleDB does not support null values,
     * this method checks each value,
     * regardless of the field.
     * 
     * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
     */
    public void processNext(Scan s) {
        Constant newMin = s.getVal(fldname);
        if (newMin.compareTo(min) < 0) {
            min = newMin;
        }
    }

    /**
     * Return the field's name, prepended by "minof".
     * 
     * @see simpledb.materialize.AggregationFn#fieldName()
     */
    public String fieldName() {
        return "minof" + fldname;
    }

    /**
     * Return the current count.
     * 
     * @see simpledb.materialize.AggregationFn#value()
     */
    public Constant value() {
        return min;
    }
}
