package simpledb.parse;

import java.util.*;
import java.io.*;

/**
 * The lexical analyzer.
 * 
 * @author Edward Sciore
 */
public class Lexer {
    private Collection<String> keywords;
    private StreamTokenizer tok;
    // ADDED
    private Collection<String> aggs;

    /**
     * Creates a new lexical analyzer for SQL statement s.
     * 
     * @param s the SQL statement
     */
    public Lexer(String s) {
        initKeywords();
        // Lab 5
        initAggs();
        tok = new StreamTokenizer(new StringReader(s));
        tok.ordinaryChar('.');   //disallow "." in identifiers
        tok.wordChars('_', '_'); //allow "_" in identifiers
        tok.lowerCaseMode(true); //ids and keywords are converted
        nextToken();
    }

    //Methods to check the status of the current token

    /**
     * Returns true if the current token is
     * the specified delimiter character.
     * 
     * @param d a character denoting the delimiter
     * @return true if the delimiter is the current token
     */
    public boolean matchDelim(char d) {
        return d == (char) tok.ttype;
    }

    /**
     * Returns true if the current token is an integer.
     * 
     * @return true if the current token is an integer
     */
    public boolean matchIntConstant() {
        return tok.ttype == StreamTokenizer.TT_NUMBER;
    }

    /**
     * Returns true if the current token is a string.
     * 
     * @return true if the current token is a string
     */
    public boolean matchStringConstant() {
        return '\'' == (char) tok.ttype;
    }

    /**
     * Returns true if the current token is the specified keyword.
     * 
     * @param w the keyword string
     * @return true if that keyword is the current token
     */
    public boolean matchKeyword(String w) {
        return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
    }

    /**
     * Returns true if the current token is a legal identifier.
     * 
     * @return true if the current token is an identifier
     */
    public boolean matchId() {
        return tok.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
    }

    // Lab 5: Aggregation.
    /**
     * Returns true if the current token is an aggregate function.
     * 
     * @return true if current token is an aggregate function
     */
    public boolean matchAgg() {
        return tok.ttype == StreamTokenizer.TT_WORD && aggs.contains(tok.sval);
    }

    // Methods to "eat" the current token

    public String eatOpr() {
        char currentChar = (char) tok.ttype;
        nextToken();
        char nextChar = (char) tok.ttype;
        switch (currentChar) {
            case '=':
                return "=";
            case '>':
                switch (nextChar) {
                    case '=':
                        nextToken();
                        return ">=";
                    default:
                        return ">";
                }
            case '<':
                switch (nextChar) {
                    case '=':
                        nextToken();
                        return "<=";
                    case '>':
                        nextToken();
                        return "<>";
                    default:
                        return "<";
                }
            case '!':
                switch (nextChar) {
                    case '=':
                        nextToken();
                        return "!=";
                    default:
                        throw new BadSyntaxException();
                }
            default:
                throw new BadSyntaxException();
        }
    }

    /**
     * Throws an exception if the current token is not the
     * specified delimiter.
     * Otherwise, moves to the next token.
     * 
     * @param d a character denoting the delimiter
     */
    public void eatDelim(char d) {
        if (!matchDelim(d))
            throw new BadSyntaxException();
        nextToken();
    }

    /**
     * Throws an exception if the current token is not
     * an integer.
     * Otherwise, returns that integer and moves to the next token.
     * 
     * @return the integer value of the current token
     */
    public int eatIntConstant() {
        if (!matchIntConstant())
            throw new BadSyntaxException();
        int i = (int) tok.nval;
        nextToken();
        return i;
    }

    /**
     * Throws an exception if the current token is not
     * a string.
     * Otherwise, returns that string and moves to the next token.
     * 
     * @return the string value of the current token
     */
    public String eatStringConstant() {
        if (!matchStringConstant())
            throw new BadSyntaxException();
        String s = tok.sval; // constants are not converted to lower case
        nextToken();
        return s;
    }

    /**
     * Throws an exception if the current token is not the
     * specified keyword.
     * Otherwise, moves to the next token.
     * 
     * @param w the keyword string
     */
    public void eatKeyword(String w) {
        if (!matchKeyword(w))
            throw new BadSyntaxException();
        nextToken();
    }

    /**
     * Throws an exception if the current token is not
     * an identifier.
     * Otherwise, returns the identifier string
     * and moves to the next token.
     * 
     * @return the string value of the current token
     */
    public String eatId() {
        if (!matchId())
            throw new BadSyntaxException();
        String s = tok.sval;
        nextToken();
        return s;
    }

    // Lab 5: Aggregation.
    /**
     * Throws an exception if the current token is not an aggregate function.
     * Otherwise, returns the aggregate function 
     * and moves to the next token.
     * 
     * @param w the aggregate function
     */
    public String eatAgg() {
        if (!matchAgg()) 
            throw new BadSyntaxException();
        String agg = tok.sval;
        nextToken();
        return agg;
    }

    /*
     * private void pushBack() {
     * try {
     * tok.pushBack();
     * }
     * catch(Exception e) {
     * System.out.println("Testing here");
     * throw new BadSyntaxException();
     * }
     * }
     */
    private void nextToken() {
        try {
            tok.nextToken();
        } catch (IOException e) {
            throw new BadSyntaxException();
        }
    }

    // All Labs 
    private void initKeywords() {
        keywords = Arrays.asList("select", "from", "where", "and",
                "insert", "into", "values", "delete", "update", "set",
                "create", "table", "int", "varchar", "view", "as", "index", "on", 
                "using", "btree", "hash", "group", "by", "order", "by", "asc", "desc", 
                "distinct");
    }

    // Lab 5 Aggregation Functions.
    private void initAggs() {
        aggs = Arrays.asList("sum", "count", "avg", "min", "max");
    }
}