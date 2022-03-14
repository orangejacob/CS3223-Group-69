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
	 * @param tx the transaction
	 * @param inner the inner scan
	 * @param outer the outer block using TempTable
	 * @param fldname1 the inner scan join field name
	 * @param fldname2 the outer scan join field name
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
	 * Initialize the chunk scan positioned before
	 * the first record for the first block.
	 * @see simpledb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		nextblknum = 0;
		useNextChunk();
	}

	/**
	 * Lab 4 - Nested Loops Join by Blocks.
	 * Continuously moving the Inner Scan and returns true
	 * whenever there is a match with the Outer Block scan.
	 * When Inner scan reaches the end, move Outer Block Scan.
	 * If Outer Scan reaches the end, load in the next Block.
	 * If reaches end of the last Block, return false.
	 */
	public boolean next() {
		boolean innerHasMore = inner.next();
		while(innerHasMore) {
			// Inner Scan matches with Outer Scan.
			if(inner.getVal(fldname1).equals(outer.getVal(fldname2)))
				return true;
			// Inner scan reaches the end.
			if (!inner.next()) {
				// Moves Outer Scan + Move back Inner Scan.
				if(outer.next()) 
					inner.beforeFirst();
				// End of Outer Scan + No next block -> Return false.
				else if(!useNextChunk()) 
					return false;
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

	/**
	 * Lab 4 - Nested Loops Join by Blocks.
	 * Utilized the MultiBuffer logic, by using Chunk Size
	 * to load in Block.
	 */
	
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

