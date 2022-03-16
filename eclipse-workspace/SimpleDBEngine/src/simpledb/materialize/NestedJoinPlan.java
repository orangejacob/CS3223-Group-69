package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;


public class NestedJoinPlan implements Plan {
	private Transaction tx;
	private Plan p1, p2, innerPlan, outerPlan;
	private String innerFldName, outerFldName;
	private Schema sch = new Schema();

	/**
	 * Creates a nested join plan for the two specified queries.
	 * Uses the Plan with lesser records as Outer and the Plan
	 * with more records as Inner, to reduce the number of BlockAccessed.
	 * @param p1 the LHS query plan
	 * @param p2 the RHS query plan
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 * @param tx the calling transaction
	 */

	public NestedJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {      
		this.tx = tx;
		this.p1 = p1;
		this.p2 = p2;
		sch.addAll(p1.schema());
		sch.addAll(p2.schema());
		if (p1.recordsOutput() < p2.recordsOutput()) {
			this.innerPlan = p2;
			this.outerPlan = p1;
			this.innerFldName = fldname2;
			this.outerFldName = fldname1;
		}else {
			this.innerPlan = p1;
			this.outerPlan = p2;
			this.innerFldName = fldname1;
			this.outerFldName = fldname2;
		}
	}

	/**
	 * Open inner scan and create a TempTable for outer scan.
	 * TempTable is to allow the use of ChunkSize, implementing
	 * Nested Block Join, hence reducing overall Block Accessed.
	 * Returns NestedJoinScan.
	 */
	public Scan open() {
		Scan inner = this.innerPlan.open();
		TempTable outer = copyRecordsFrom(new MaterializePlan(tx, this.outerPlan));
		return new NestedJoinScan(tx, inner, innerFldName, outer, outerFldName);
	}

	/**
	 * Return the number of block acceses required to
	 * nested block join the tables.
	 * Formula: |Outer| + Ceil(|Outer| / Block Size) * |Inner|. 
	 * It does <i>not</i> include the one-time cost
	 * of materializing the records.
	 * @see simpledb.plan.Plan#blocksAccessed()
	 */
	public int blocksAccessed() {
		int blockSize  = tx.availableBuffs() - 2; // 1 for Input, 1 for output.
		int innerPages = innerPlan.blocksAccessed();
		int outerPages = outerPlan.blocksAccessed();
		return (int) Math.round(outerPages + Math.ceil(outerPages / blockSize) * innerPages);
	}

	/**
	 * Estimates the number of output records.
	 * For Nested Block, at most it will be inner * outer.
	 */
	public int recordsOutput() {
		return innerPlan.recordsOutput() * outerPlan.recordsOutput();
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
	
	
	// Lab 4: Logic from MultiBuffer, creates a TempTable from Plan.
	private TempTable copyRecordsFrom(Plan p) {
		Scan   src = p.open(); 
		Schema sch = p.schema();
		TempTable t = new TempTable(tx, sch);
		UpdateScan dest = (UpdateScan) t.open();
		while (src.next()) {
			dest.insert();
			for (String fldname : sch.fields())
				dest.setVal(fldname, src.getVal(fldname));
		}
		src.close();
		dest.close();
		return t;
	}
}

