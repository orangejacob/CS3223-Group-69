package simpledb.opt;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
	private String tblname;
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
		this.tblname = tblname;
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
		String leftAlignFormat = "| %-20s | %-4d |%n";
		String comparatorType = null, cheapestPlanName = null; ;
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);

		if (joinpred == null)
			return null;

		LinkedHashMap<String, Plan> joins = new LinkedHashMap<>();
		joins.put("Product", makeProductJoin(current, currsch));
		joins.put("Nested", makeNestedBlockJoin(current, currsch, joinpred));

		for(String fldname: currsch.fields()) {
			String comparator = joinpred.fieldComparatorType(fldname);
			if(comparator != null) {
				comparatorType = comparator;
				break;
			}
		}
		
		if(comparatorType.equals("=")) {
			joins.put("Index",  makeIndexJoin(current, currsch));
			joins.put("Hash", makeHashJoin(current, currsch, joinpred));
			joins.put("Sort Merge", makeMergeJoin(current, currsch, joinpred));
		}

		System.out.format("+----------------------+------+%n");
		System.out.format("| Join Type            | Cost |%n");
		System.out.format("+----------------------+------+%n");
		
		Plan curPlan = null;
		Plan cheapestPlan = null;
		
		// Iterating HashMap through for loop
        for (Map.Entry<String, Plan> set : joins.entrySet()) {
        	curPlan = set.getValue();
        	if(curPlan == null) {
        		continue;
        	}
        	System.out.format(leftAlignFormat, set.getKey(), curPlan.blocksAccessed());
			if (cheapestPlan == null || curPlan.blocksAccessed() < cheapestPlan.blocksAccessed()) {
				cheapestPlanName = set.getKey();
				cheapestPlan = curPlan;
			}
        }
       

		System.out.format("+----------------------+------+%n");
		System.out.format("----------------------------------------%n");
		System.out.format(" %-22s %n", "Selected Join Type: " + cheapestPlanName);
		System.out.format("----------------------------------------%n");
		return cheapestPlan;

	}

	// For Experiment 2
	public Plan makeJoinPlanManual(Plan current, String joinName) {

		Schema currsch = current.schema();
		Predicate joinpred = mypred.joinSubPred(myschema, currsch);

		if (joinpred == null)
			return null;

		switch (joinName) {
		case "index":
			System.out.println("Used Index Join");
			return makeIndexJoin(current, currsch);
		case "merge":
			System.out.println("Used Sort Merge Join");
			return makeMergeJoin(current, currsch, joinpred);
		case "hash":
			System.out.println("Used Hash Join");
			return makeHashJoin(current, currsch, joinpred);
		case "nested":
			System.out.println("Used Nested Loops Join");
			return makeNestedBlockJoin(current, currsch, joinpred);
		default:
			System.out.println("Used Product Join");
			return	makeProductJoin(current, currsch);  
		}

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
				if(ii.getIndexType().equals("hash") || ii.getIndexType().equals("btree")) {
					String comparatorType = mypred.fieldComparatorTypeByConstant(fldname);
					if(comparatorType != null && !comparatorType.equals("=")) 
						return null;
					System.out.println(comparatorType);
				}
				System.out.println(ii.getIndexType() + " index on " + fldname + " used");
				return new IndexSelectPlan(myplan, ii, val, tblname);
			}
		}
		return null;
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
		Plan p = new NestedJoinPlan(tx, current, myplan, joinpred);
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
