package simpledb.parse;

import java.util.*;

import simpledb.query.*;
import simpledb.materialize.AggregationFn;

/**
 * Data for the SQL <i>select</i> statement.
 * 
 * @author Edward Sciore
 */
public class QueryData {
    private List<String> fields;
    private Collection<String> tables;
    private Predicate pred;
    // ADDED
    private List<AggregationFn> aggs;
    private List<String> groupfields;

    // CHANGED
    /**
     * Saves the field and table list and predicate.
     */
    public QueryData(List<String> fields, List<AggregationFn> aggs, Collection<String> tables, Predicate pred,
            List<String> groupfields) {
        this.fields = fields;
        this.aggs = aggs;
        this.tables = tables;
        this.pred = pred;
        this.groupfields = groupfields;
    }

    /**
     * Returns the fields mentioned in the select clause.
     * 
     * @return a list of field names
     */
    public List<String> fields() {
        return fields;
    }

    // ADDED
    /**
     * Returns the aggregation functions mentioned in the select clause.
     * 
     * @return a list of aggregation functions
     */
    public List<AggregationFn> aggs() {
        return aggs;
    }

    /**
     * Returns the tables mentioned in the from clause.
     * 
     * @return a collection of table names
     */
    public Collection<String> tables() {
        return tables;
    }

    /**
     * Returns the predicate that describes which
     * records should be in the output table.
     * 
     * @return the query predicate
     */
    public Predicate pred() {
        return pred;
    }

    // ADDED
    /**
     * Returns the group fields in the group by clause.
     * 
     * @return a list of group fields
     */
    public List<String> groupfields() {
        return groupfields;
    }

    public String toString() {
        String result = "select ";
        for (String fldname : fields)
            result += fldname + ", ";
        result = result.substring(0, result.length() - 2); // remove final comma
        result += " from ";
        for (String tblname : tables)
            result += tblname + ", ";
        result = result.substring(0, result.length() - 2); // remove final comma
        String predstring = pred.toString();
        if (!predstring.equals(""))
            result += " where " + predstring;
        return result;
    }
}
