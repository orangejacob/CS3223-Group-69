package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;


public class HashJoinPlan implements Plan {
	private Plan p1, p2;
	private int totalPartitions;
	private String fldname1, fldname2;
	private Schema sch = new Schema();
	
	/**
	 * Creates a Hash Join plan for the two specified queries.
	 * @param tx the calling transaction
	 * @param p1 the LHS query plan
	 * @param p2 the RHS query plan
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 */

	public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {      
		this.p1 = p1;
		this.p2 = p2;
		sch.addAll(p1.schema());
		sch.addAll(p2.schema());
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.totalPartitions = tx.availableBuffs() - 1; // 1 for Input page.
	}

	/** 
	 * Lab 05: Hash Based Join.
	 * Returns HashJoinScan.
	 */
	public Scan open() {
		Scan s1 = p1.open();
		Scan s2 = p2.open();
		return new HashJoinScan(s1, fldname1, s2, fldname2, totalPartitions, p1.schema().fields());
	}


	/**
	 * Return the number of block acceses required to
	 * hash join the tables.
	 * Formula: 3 * (|P1| + |P2|).
	 * @see simpledb.plan.Plan#blocksAccessed()
	 */
	public int blocksAccessed() {
		return 3 * (p1.blocksAccessed() + p2.blocksAccessed());
	}

	/**
	 * Return the number of records in the join.
	 * Assuming uniform distribution, the formula is:
	 * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
	 * @see simpledb.plan.Plan#recordsOutput()
	 */
	public int recordsOutput() {
		return p1.recordsOutput() * p2.recordsOutput();
	}

	/**
	 * Estimate the distinct number of field values in the join.
	 * Since the join does not increase or decrease field values,
	 * the estimate is the same as in the appropriate underlying query.
	 * @see simpledb.plan.Plan#distinctValues(java.lang.String)
	 */
	public int distinctValues(String fldname) {
		if (p1.schema().hasField(fldname))
			return p1.distinctValues(fldname);
		else
			return p2.distinctValues(fldname);
	}

	/**
	 * Return the schema of the join,
	 * which is the union of the schemas of the underlying queries.
	 * @see simpledb.plan.Plan#schema()
	 */
	public Schema schema() {
		return sch;
	}

	
}

