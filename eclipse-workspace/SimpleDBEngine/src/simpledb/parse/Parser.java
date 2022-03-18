package simpledb.parse;

import java.util.*;

import simpledb.query.*;
import simpledb.record.*;
import simpledb.materialize.*;

/**
 * The SimpleDB parser.
 * 
 * @author Edward Sciore
 */
public class Parser {
	private Lexer lex;

	public Parser(String s) {
		lex = new Lexer(s);
	}

	// Methods for parsing predicates, terms, expressions, constants, and fields

	public String field() {
		return lex.eatId();
	}

	public Constant constant() {
		if (lex.matchStringConstant())
			return new Constant(lex.eatStringConstant());
		else
			return new Constant(lex.eatIntConstant());
	}

	public Expression expression() {
		if (lex.matchId())
			return new Expression(field());
		else
			return new Expression(constant());
	}

	public Term term() {
		Expression lhs = expression();
		String operator = lex.eatOpr();
		Expression rhs = expression();
		return new Term(lhs, rhs, operator);
	}

	public Predicate predicate() {
		Predicate pred = new Predicate(term());
		if (lex.matchKeyword("and")) {
			lex.eatKeyword("and");
			pred.conjoinWith(predicate());
		}
		return pred;
	}

	// Lab 5: Aggregation Field.
	public String agg() {
		return lex.eatAgg();
	}

	// Methods for parsing queries
	public QueryData query() {

		boolean distinctQuery = false;
		LinkedHashMap<String, Integer> sortFields = new LinkedHashMap<>();

		lex.eatKeyword("select");

		if (lex.matchKeyword("distinct")) {
			lex.eatKeyword("distinct");
			distinctQuery = true;
		}

		// Lab 5: New Fields Array.
		List<String> fields = new ArrayList<>();

		// Lab 5: Aggregation function.
		List<AggregationFn> aggs = new ArrayList<>();
		while (true) {
			String field;
			if (lex.matchAgg()) {
				String agg = agg();
				lex.eatDelim('(');
				field = field();
				lex.eatDelim(')');
				aggs.add(getAggType(agg, field));
			} else 
				field = field();
			fields.add(field);
			if (!lex.matchDelim(','))
				break;
			else 
				lex.eatDelim(',');
		}

		lex.eatKeyword("from");
		Collection<String> tables = tableList();
		Predicate pred = new Predicate();

		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}

		// Lab 5: Aggregation and Group by.
		List<String> groupfields = new ArrayList<>();
		if (lex.matchKeyword("group")) {
			lex.eatKeyword("group");
			lex.eatKeyword("by");
			groupfields = groupfieldList();
		}

		if (lex.matchKeyword("order")) {
			lex.eatKeyword("order");
			lex.eatKeyword("by");
			sortFields = selectSortList();
		}
		return new QueryData(fields, tables, pred, sortFields, distinctQuery, aggs, groupfields);
	}

	// Lab 5: Aggregation Field.
	private AggregationFn getAggType(String agg, String field) {
		switch (agg.toLowerCase()) {
		case "sum" -> {
			return new SumFn(field);
		}
		case "count" -> {
			return new CountFn(field);
		}
		case "avg" -> {
			return new AvgFn(field);
		}
		case "min" -> {
			return new MinFn(field);
		}
		case "max" -> {
			return new MaxFn(field);
		}
		}
		return null;
	}

	// Lab 5: Group by Field.
	private List<String> groupfieldList() {
		List<String> groupfields = new ArrayList<>();
		groupfields.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			groupfields.addAll(groupfieldList());
		}
		return groupfields;
	}

	/*
	 * Lab 3: Sort Plan
	 * Purpose: Recursively match & eat 
	 * field names to be sorted on, as
	 * well as the sorting type.
	 * By default, sorting type is ascending (1).
	 * */
	private LinkedHashMap<String, Integer> selectSortList(){
		LinkedHashMap<String, Integer> L = new LinkedHashMap<>();
		String field = field();
		L.put(field, 1);
		if(lex.matchKeyword("desc")) {
			lex.eatKeyword("desc");
			L.put(field, -1);
		} else if (lex.matchKeyword("asc")) {
			lex.eatKeyword("asc");
		}

		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.putAll(selectSortList());
		}
		return L;
	}

	private List<String> selectList() {
		List<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(selectList());
		}
		return L;
	}

	private Collection<String> tableList() {
		Collection<String> L = new ArrayList<String>();
		L.add(lex.eatId());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(tableList());
		}
		return L;
	}

	// Methods for parsing the various update commands

	public Object updateCmd() {
		if (lex.matchKeyword("insert"))
			return insert();
		else if (lex.matchKeyword("delete"))
			return delete();
		else if (lex.matchKeyword("update"))
			return modify();
		else
			return create();
	}

	private Object create() {
		lex.eatKeyword("create");
		if (lex.matchKeyword("table"))
			return createTable();
		else if (lex.matchKeyword("view"))
			return createView();
		else
			return createIndex();
	}

	// Method for parsing delete commands

	public DeleteData delete() {
		lex.eatKeyword("delete");
		lex.eatKeyword("from");
		String tblname = lex.eatId();
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}
		return new DeleteData(tblname, pred);
	}

	// Methods for parsing insert commands

	public InsertData insert() {
		lex.eatKeyword("insert");
		lex.eatKeyword("into");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		List<String> flds = fieldList();
		lex.eatDelim(')');
		lex.eatKeyword("values");
		lex.eatDelim('(');
		List<Constant> vals = constList();
		lex.eatDelim(')');
		return new InsertData(tblname, flds, vals);
	}

	private List<String> fieldList() {
		List<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(fieldList());
		}
		return L;
	}

	private List<Constant> constList() {
		List<Constant> L = new ArrayList<Constant>();
		L.add(constant());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(constList());
		}
		return L;
	}

	// Method for parsing modify commands

	public ModifyData modify() {
		lex.eatKeyword("update");
		String tblname = lex.eatId();
		lex.eatKeyword("set");
		String fldname = field();
		lex.eatDelim('=');
		Expression newval = expression();
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate();
		}
		return new ModifyData(tblname, fldname, newval, pred);
	}

	// Method for parsing create table commands

	public CreateTableData createTable() {
		lex.eatKeyword("table");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		Schema sch = fieldDefs();
		lex.eatDelim(')');
		return new CreateTableData(tblname, sch);
	}

	private Schema fieldDefs() {
		Schema schema = fieldDef();
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			Schema schema2 = fieldDefs();
			schema.addAll(schema2);
		}
		return schema;
	}

	private Schema fieldDef() {
		String fldname = field();
		return fieldType(fldname);
	}

	private Schema fieldType(String fldname) {
		Schema schema = new Schema();
		if (lex.matchKeyword("int")) {
			lex.eatKeyword("int");
			schema.addIntField(fldname);
		}
		else {
			lex.eatKeyword("varchar");
			lex.eatDelim('(');
			int strLen = lex.eatIntConstant();
			lex.eatDelim(')');
			schema.addStringField(fldname, strLen);
		}
		return schema;
	}

	// Method for parsing create view commands

	public CreateViewData createView() {
		lex.eatKeyword("view");
		String viewname = lex.eatId();
		lex.eatKeyword("as");
		QueryData qd = query();
		return new CreateViewData(viewname, qd);
	}


	//  Method for parsing create index commands

	public CreateIndexData createIndex() {
		lex.eatKeyword("index");
		String idxname = lex.eatId();
		lex.eatKeyword("on");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		String fldname = field();
		lex.eatDelim(')');
		lex.eatKeyword("using");

		//Main add on for lab 2 here
		String mtdname;
		if (lex.matchKeyword("btree")) {
			lex.eatKeyword("btree");
			mtdname = "btree";
		} else {
			lex.eatKeyword("hash");
			mtdname = "hash";
		}
		return new CreateIndexData(idxname, tblname, fldname, mtdname);
	}


	/* Methods for parsing the various update commands

    public Object updateCmd() {
        if (lex.matchKeyword("insert"))
            return insert();
        else if (lex.matchKeyword("delete"))
            return delete();
        else if (lex.matchKeyword("update"))
            return modify();
        else
            return create();
    }

    private Object create() {
        lex.eatKeyword("create");
        if (lex.matchKeyword("table"))
            return createTable();
        else if (lex.matchKeyword("view"))
            return createView();
        else
            return createIndex();
    }

    // Method for parsing delete commands

    public DeleteData delete() {
        lex.eatKeyword("delete");
        lex.eatKeyword("from");
        String tblname = lex.eatId();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new DeleteData(tblname, pred);
    }

    // Methods for parsing insert commands

    public InsertData insert() {
        lex.eatKeyword("insert");
        lex.eatKeyword("into");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        List<String> flds = fieldList();
        lex.eatDelim(')');
        lex.eatKeyword("values");
        lex.eatDelim('(');
        List<Constant> vals = constList();
        lex.eatDelim(')');
        return new InsertData(tblname, flds, vals);
    }

    private List<String> fieldList() {
        List<String> L = new ArrayList<String>();
        L.add(field());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(fieldList());
        }
        return L;
    }

    private List<Constant> constList() {
        List<Constant> L = new ArrayList<Constant>();
        L.add(constant());
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            L.addAll(constList());
        }
        return L;
    }

    // Method for parsing modify commands

    public ModifyData modify() {
        lex.eatKeyword("update");
        String tblname = lex.eatId();
        lex.eatKeyword("set");
        String fldname = field();
        lex.eatDelim('=');
        Expression newval = expression();
        Predicate pred = new Predicate();
        if (lex.matchKeyword("where")) {
            lex.eatKeyword("where");
            pred = predicate();
        }
        return new ModifyData(tblname, fldname, newval, pred);
    }

    // Method for parsing create table commands

    public CreateTableData createTable() {
        lex.eatKeyword("table");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        Schema sch = fieldDefs();
        lex.eatDelim(')');
        return new CreateTableData(tblname, sch);
    }

    private Schema fieldDefs() {
        Schema schema = fieldDef();
        if (lex.matchDelim(',')) {
            lex.eatDelim(',');
            Schema schema2 = fieldDefs();
            schema.addAll(schema2);
        }
        return schema;
    }

    private Schema fieldDef() {
        String fldname = field();
        return fieldType(fldname);
    }

    private Schema fieldType(String fldname) {
        Schema schema = new Schema();
        if (lex.matchKeyword("int")) {
            lex.eatKeyword("int");
            schema.addIntField(fldname);
        } else {
            lex.eatKeyword("varchar");
            lex.eatDelim('(');
            int strLen = lex.eatIntConstant();
            lex.eatDelim(')');
            schema.addStringField(fldname, strLen);
        }
        return schema;
    }

    // Method for parsing create view commands

    public CreateViewData createView() {
        lex.eatKeyword("view");
        String viewname = lex.eatId();
        lex.eatKeyword("as");
        QueryData qd = query();
        return new CreateViewData(viewname, qd);
    }

    // Method for parsing create index commands

    public CreateIndexData createIndex() {
        lex.eatKeyword("index");
        String idxname = lex.eatId();
        lex.eatKeyword("on");
        String tblname = lex.eatId();
        lex.eatDelim('(');
        String fldname = field();
        lex.eatDelim(')');
        lex.eatKeyword("using");

        // Main add on for lab 2 here
        String mtdname;
        if (lex.matchKeyword("btree")) {
            lex.eatKeyword("btree");
            mtdname = "btree";
        } else {
            lex.eatKeyword("hash");
            mtdname = "hash";
        }
        return new CreateIndexData(idxname, tblname, fldname, mtdname);
    }*/

}
