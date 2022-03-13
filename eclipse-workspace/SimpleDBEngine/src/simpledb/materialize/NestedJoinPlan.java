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
	 * Creates a mergejoin plan for the two specified queries.
	 * The RHS must be materialized after it is sorted, 
	 * in order to deal with possible duplicates.
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
		this.outerPlan = p1.recordsOutput() < p2.recordsOutput() ? p1 : p2;
		this.innerPlan = p1.recordsOutput() < p2.recordsOutput() ? p2 : p1;
		
		if (p1.recordsOutput() < p2.recordsOutput()) {
			this.outerPlan = p1;
			this.outerFldName = fldname1;
			this.innerPlan = p2;
			this.innerFldName = fldname2;
		}else {
			this.innerPlan = p1;
			this.innerFldName = fldname1;
			this.outerPlan = p2;
			this.outerFldName = fldname2;
		}
	}

	/** The method first sorts its two underlying scans
	 * on their join field. It then returns a mergejoin scan
	 * of the two sorted table scans.
	 * @see simpledb.plan.Plan#open()
	 */
	public Scan open() {
		Scan inner = this.innerPlan.open();
		TempTable outer = copyRecordsFrom(new MaterializePlan(tx, this.outerPlan));
		return new NestedJoinScan(tx, inner, innerFldName, outer, outerFldName);
	}

	/**
	 * Return the number of block acceses required to
	 * mergejoin the sorted tables.
	 * Since a mergejoin can be preformed with a single
	 * pass through each table, the method returns
	 * the sum of the block accesses of the 
	 * materialized sorted tables.
	 * It does <i>not</i> include the one-time cost
	 * of materializing and sorting the records.
	 * @see simpledb.plan.Plan#blocksAccessed()
	 */
	public int blocksAccessed() {
		return (int) Math.round(outerPlan.blocksAccessed() + 
				(Math.ceil(outerPlan.blocksAccessed() / (tx.availableBuffs() - 2)) * innerPlan.blocksAccessed()));
	}

	/**
	 * Return the number of records in the join.
	 * Assuming uniform distribution, the formula is:
	 * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
	 * @see simpledb.plan.Plan#recordsOutput()
	 */
	public int recordsOutput() {
		return this.outerPlan.recordsOutput() * this.outerPlan.recordsOutput();
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

