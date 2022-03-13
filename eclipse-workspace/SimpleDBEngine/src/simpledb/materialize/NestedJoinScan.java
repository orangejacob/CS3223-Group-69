package simpledb.materialize;

import simpledb.tx.*;
import simpledb.multibuffer.BufferNeeds;
import simpledb.multibuffer.ChunkScan;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;


public class NestedJoinScan implements Scan {

	private Scan inner;
	private ChunkScan outer;

	private Layout layout;
	private Transaction tx;
	private String filename;

	private String fldname1, fldname2;
	private int chunksize, nextblknum, filesize;

	/**
	 * Create a nested join scan for the two scans.
	 * @param s1 the LHS sorted scan
	 * @param s2 the RHS sorted scan
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 */
	public NestedJoinScan(Transaction tx, Scan inner, String fldname1, TempTable tt, String fldname2) {
		this.tx = tx;
		this.inner = inner;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.filename = tt.tableName() + ".tbl";
		this.filesize = tx.size(this.filename);
		this.layout   = tt.getLayout();
		int available = tx.availableBuffs();
		this.chunksize = BufferNeeds.bestFactor(available, filesize);
		beforeFirst();
	}

	/**
	 * Close the scan by closing the two underlying scans.
	 * @see simpledb.query.Scan#close()
	 */
	public void close() {
		inner.close();
		outer.close();
	}

	/**
	 * Position the scan before the first record,
	 * by positioning each underlying scan before
	 * their first records.
	 * @see simpledb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		nextblknum = 0;
		useNextChunk();
	}

	/**
	 * Lab 4 - Keep moving Inner Table (S2), returns true whenever there's a match with Outer (S1).
	 * When S2 finishes -> Move forward S1 record, and continue.
	 */
	public boolean next() {
		boolean innerHasMore = inner.next();
		while(innerHasMore) {
			if(inner.getVal(fldname1).equals(outer.getVal(fldname2))){
				return true;
			}
			// End of Inner Scan
			if (!inner.next()) {
				// Move to Outer Scan next.
				if(outer.next()) {
					inner.beforeFirst();
				}else if(!useNextChunk()) {
					return false;
				}
				innerHasMore = inner.next();
			}
		}
		return false;
	}

	/** 
	 * Return the integer value of the specified field.
	 * The value is obtained from whichever scan
	 * contains the field.
	 * @see simpledb.query.Scan#getInt(java.lang.String)
	 */
	public int getInt(String fldname) {
		if (inner.hasField(fldname))
			return outer.getInt(fldname);
		else
			return outer.getInt(fldname);
	}

	/** 
	 * Return the string value of the specified field.
	 * The value is obtained from whichever scan
	 * contains the field.
	 * @see simpledb.query.Scan#getString(java.lang.String)
	 */
	public String getString(String fldname) {
		if (inner.hasField(fldname))
			return inner.getString(fldname);
		else
			return outer.getString(fldname);
	}

	/** 
	 * Return the value of the specified field.
	 * The value is obtained from whichever scan
	 * contains the field.
	 * @see simpledb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		if (inner.hasField(fldname))
			return inner.getVal(fldname);
		else
			return outer.getVal(fldname);
	}

	/**
	 * Return true if the specified field is in
	 * either of the underlying scans.
	 * @see simpledb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		return inner.hasField(fldname) || outer.hasField(fldname);
	}

	private boolean useNextChunk() {
		if (nextblknum >= filesize)
			return false;
		if (outer != null)
			outer.close();
		int end = nextblknum + chunksize - 1;
		if (end >= filesize)
			end = filesize - 1;
		this.outer = new ChunkScan(tx, filename, layout, nextblknum, end);
		inner.beforeFirst();
		outer.beforeFirst();
		outer.next();
		nextblknum = end + 1;
		return true;
	}
}

