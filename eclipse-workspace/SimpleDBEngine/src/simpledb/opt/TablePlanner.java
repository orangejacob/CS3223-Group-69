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
	private double recorder;
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
		System.out.println(tblname);
		indexes  = mdm.getIndexInfo(tblname, tx);
		for(String f: indexes.keySet()) {
			System.out.println("LMAO " + f);
		}
		recorder = Math.random();
		System.out.println(recorder);
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
		String leftAlignFormat = "| %-20s | %-4d |%n";
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);
		for(String f: indexes.keySet()) {
			System.out.println("LMAO 2 " + f);
		}
		System.out.println(recorder);
		
		if (joinpred == null)
			return null;
		

		System.out.format("+----------------------+------+%n");
		System.out.format("| Join Type            | Cost |%n");
		System.out.format("+----------------------+------+%n");
		String cheapestPlanName = "Product";
		
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
			System.out.format(leftAlignFormat, "Product", cheapestPlan.blocksAccessed());
		
		if (mergePlan != null) {
			System.out.format(leftAlignFormat, "Sort Merge", mergePlan.blocksAccessed());
			if (mergePlan.blocksAccessed() < cheapestPlan.blocksAccessed()) {
				cheapestPlanName = "Sort Merge";
				cheapestPlan = mergePlan;
			}
		}
		
		if (indexPlan != null) {
			System.out.format(leftAlignFormat, "Indexed", indexPlan.blocksAccessed());
			if (indexPlan.blocksAccessed() < cheapestPlan.blocksAccessed()) {
				cheapestPlanName = "Indexed Plan";
				cheapestPlan = indexPlan;
			}
		}else {
			System.out.println("Index not working.");
		}
		
		if (nestedPlan != null) {
			System.out.format(leftAlignFormat, "Nested Loops", nestedPlan.blocksAccessed());
			if (nestedPlan.blocksAccessed() < cheapestPlan.blocksAccessed()) {
				cheapestPlanName = "Nested Loops";
				cheapestPlan = nestedPlan;
			}
		}
		
		if (hashPlan != null) {
			System.out.format(leftAlignFormat, "Hashed", hashPlan.blocksAccessed());
			if (hashPlan.blocksAccessed() < cheapestPlan.blocksAccessed()) {
				cheapestPlanName = "Hashed";
				cheapestPlan = hashPlan;
			}
		}
		System.out.format("+----------------------+------+%n");
		System.out.format("| %-22s |%n", "Selected Join Type: " + cheapestPlanName);
		System.out.format("+----------------------+------+%n");
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
	
	private Plan makeIndexJoin(Plan current, Schema currsch) {
		System.out.println("HERE");
		for (String fldname : indexes.keySet()) {
			System.out.println(fldname);
			String outerfield = mypred.equatesWithField(fldname);
			if (outerfield != null && currsch.hasField(outerfield)) {
				System.out.println("There's Index");
				IndexInfo ii = indexes.get(fldname);
				Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
				p = addSelectPred(p);
				return addJoinPred(p, currsch);
			}
			
		}
		System.out.println("There's no Index");
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
		Plan p = null;
		String[] joinedFields = joinpred.toString().split("=");
		if(current.schema().hasField(joinedFields[0]) && myplan.schema().hasField(joinedFields[1])) {			
			p = new MergeJoinPlan(tx, current, myplan, joinedFields[0], joinedFields[1]);
		}else if(current.schema().hasField(joinedFields[1]) && myplan.schema().hasField(joinedFields[0])) {
			p = new MergeJoinPlan(tx, current, myplan, joinedFields[1], joinedFields[0]);
		}else {
			return null;
		}
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
		Plan p = null;
		String[] joinedFields = joinpred.toString().split("=");
		if(current.schema().hasField(joinedFields[0]) && myplan.schema().hasField(joinedFields[1])) {			
			p = new NestedJoinPlan(tx, current, myplan, joinedFields[0], joinedFields[1]);
		}else if(current.schema().hasField(joinedFields[1]) && myplan.schema().hasField(joinedFields[0])) {
			p = new NestedJoinPlan(tx, current, myplan, joinedFields[1], joinedFields[0]);
		}else {
			return null;
		}
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
		// Split the join predicate. 
		Plan p = null;
		String[] joinedFields = joinpred.toString().split("=");
		if(current.schema().hasField(joinedFields[0]) && myplan.schema().hasField(joinedFields[1])) {			
			p = new HashJoinPlan(tx, current, myplan, joinedFields[0], joinedFields[1]);
		}else if(current.schema().hasField(joinedFields[1]) && myplan.schema().hasField(joinedFields[0])) {
			p = new HashJoinPlan(tx, current, myplan, joinedFields[1], joinedFields[0]);
		}else {
			return null;
		}
		p = addSelectPred(p);
		return addJoinPred(p, currsch);
	}
	
	
	
}
