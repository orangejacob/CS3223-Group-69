package simpledb.opt;

import java.util.Map;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.index.planner.*;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.*;
import simpledb.materialize.*;

/**
 * This class contains methods for planning a single table.
 * @author Edward Sciore
 */
class TablePlanner {
	private TablePlan myplan;
	private Predicate mypred;
	private Schema myschema;
	private Map<String,IndexInfo> indexes;
	private Transaction tx;

	/**
	 * Creates a new table planner.
	 * The specified predicate applies to the entire query.
	 * The table planner is responsible for determining
	 * which portion of the predicate is useful to the table,
	 * and when indexes are useful.
	 * @param tblname the name of the table
	 * @param mypred the query predicate
	 * @param tx the calling transaction
	 */
	public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
		this.mypred  = mypred;
		this.tx  = tx;
		myplan   = new TablePlan(tx, tblname, mdm);
		myschema = myplan.schema();
		indexes  = mdm.getIndexInfo(tblname, tx);
	}

	/**
	 * Constructs a select plan for the table.
	 * The plan will use an indexselect, if possible.
	 * @return a select plan for the table.
	 */
	public Plan makeSelectPlan() {
		Plan p = makeIndexSelect();
		if (p == null)
			p = myplan;
		return addSelectPred(p);
	}

	/**
	 * Constructs a join plan of the specified plan
	 * and the table.  The plan will use an indexjoin, if possible.
	 * (Which means that if an indexselect is also possible,
	 * the indexjoin operator takes precedence.)
	 * The method returns null if no join is possible.
	 * @param current the specified plan
	 * @return a join plan of the plan and this table
	 */

	// Calculate all possible plan, then return the cheapest option based on block access.
	public Plan makeJoinPlan(Plan current) {

		Schema currsch = current.schema();
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);
		if (joinpred == null)
			return null;
		
		// Default: Make Product Plan -> can't fail.
		Plan cheapestPlan = makeProductJoin(current, currsch);  
		// Lab 4: Merge Join.
		Plan mergePlan    = makeMergeJoin(current, currsch, joinpred);
		// Lab 4: Index Based Join.
		Plan indexPlan    = makeIndexJoin(current, currsch);
		// Lab 4: Nested Loop Join
		Plan nestedPlan   = makeNestedBlockJoin(current, currsch, joinpred);
		// Lab 5: Hash Partition Join
		Plan hashPlan     = makeHashJoin(current, currsch, joinpred);
		
		// Find the cheapest plan out of the above, based on Block Accessed.
		if (cheapestPlan != null)
			System.out.println("Product Join Cost: " + cheapestPlan.blocksAccessed());
		
		if (mergePlan != null) {
			System.out.println("Merge Sort Join Cost: " + mergePlan.blocksAccessed());
			if (mergePlan.blocksAccessed() > cheapestPlan.blocksAccessed())
				cheapestPlan = mergePlan;
		}
		
		if (indexPlan != null) {
			System.out.println("Index Based Join Cost: " + indexPlan.blocksAccessed());
			if (indexPlan.blocksAccessed() > cheapestPlan.blocksAccessed())
				cheapestPlan = indexPlan;
		}
		
		if (nestedPlan != null) {
			System.out.println("Nested Loops Join Cost: " + nestedPlan.blocksAccessed());
			if (nestedPlan.blocksAccessed() > cheapestPlan.blocksAccessed())
				cheapestPlan = nestedPlan;
		}
		
		if (hashPlan != null) {
			System.out.println("Hash Join Cost: " + hashPlan.blocksAccessed());
			if (hashPlan.blocksAccessed() > cheapestPlan.blocksAccessed())
				cheapestPlan = hashPlan;
		}

		return cheapestPlan;
	}

	private Plan addSelectPred(Plan p) {
		Predicate selectpred = mypred.selectSubPred(myschema);
		if (selectpred != null)
			return new SelectPlan(p, selectpred);
		else
			return p;
	}

	private Plan addJoinPred(Plan p, Schema currsch) {
		Predicate joinpred = mypred.joinSubPred(currsch, myschema);
		if (joinpred != null)
			return new SelectPlan(p, joinpred);
		else
			return p;
	}

	private Plan makeIndexSelect() {
		for (String fldname : indexes.keySet()) {
			Constant val = mypred.equatesWithConstant(fldname);
			if (val != null) {
				IndexInfo ii = indexes.get(fldname);
				// If indexType is hash, only execute for equality predicate.
				if(ii.getIndexType().equals("hash")) {
					String comparatorType = mypred.fieldComparatorType(fldname);
					if(comparatorType != null && !comparatorType.equals("=")) 
						return null;
				}
				System.out.println("index on " + fldname + " used");
				return new IndexSelectPlan(myplan, ii, val);
			}
		}
		return null;
	}

	/**
	 * Constructs a product plan of the specified plan and
	 * this table.
	 * @param current the specified plan
	 * @return a product plan of the specified plan and this table
	 */
	public Plan makeProductPlan(Plan current) {
		Plan p = addSelectPred(myplan);
		return new MultibufferProductPlan(tx, current, p);
	}
	
	private Plan makeProductJoin(Plan current, Schema currsch) {
		Plan p = makeProductPlan(current);
		return addJoinPred(p, currsch);
	}
	
	
	private Plan makeIndexJoin(Plan current, Schema currsch) {
		for (String fldname : indexes.keySet()) {
			String outerfield = mypred.equatesWithField(fldname);
			if (outerfield != null && currsch.hasField(outerfield)) {
				IndexInfo ii = indexes.get(fldname);
				Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
				p = addSelectPred(p);
				return addJoinPred(p, currsch);
			}
		}
		return null;
	}

	/**
	 * Lab 4 - Sort Merge Join based on Joined Fields
	 * Constructs a merge plan of the specified plan and
	 * this table.
	 * @param current the specified plan
	 * @param currsch the specified schema
	 * @param joinpred the joined predicate
	 * @return a merge plan of the specified plan and this table
	 */
	private Plan makeMergeJoin(Plan current, Schema currsch, Predicate joinpred) {
		// Split the join predicate. 
		String[] joinedFields = joinpred.toString().split("=");
		Plan p = new MergeJoinPlan(tx, current, myplan, joinedFields[0], joinedFields[1]);
		p = addSelectPred(p);
		return addJoinPred(p, currsch);
	}
	
	/**
	 * Lab 4 - Nested Loops Join based on Joined Fields
	 * Constructs a Nested Loops plan of the specified plan and
	 * this table.
	 * @param current the specified plan
	 * @param currsch the specified schema
	 * @param joinpred the joined predicate
	 * @return a Nested Block plan of the specified plan and this table
	 */
	private Plan makeNestedBlockJoin(Plan current, Schema currsch, Predicate joinpred) {
		// Split the join predicate. 
		String[] joinedFields = joinpred.toString().split("=");
		Plan p = new NestedJoinPlan(tx, current, myplan, joinedFields[0], joinedFields[1]);
		p = addSelectPred(p);
		return addJoinPred(p, currsch);
	}

	/**
	 * Lab 5 - Hash Join based on Joined Fields
	 * Constructs a merge plan of the specified plan and
	 * this table.
	 * @param current the specified plan
	 * @param currsch the specified schema
	 * @param joinpred the joined predicate
	 * @return a hash join plan of the specified plan and this table
	 */
	private Plan makeHashJoin(Plan current, Schema currsch, Predicate joinpred) {
		// Split the join predicate. 
		String[] joinedFields = joinpred.toString().split("=");
		System.out.println(joinedFields[0] + joinedFields[1]);
		Plan p = new HashJoinPlan(tx, current, myplan, joinedFields[0], joinedFields[1]);
		p = addSelectPred(p);
		return addJoinPred(p, currsch);
	}
	

}
