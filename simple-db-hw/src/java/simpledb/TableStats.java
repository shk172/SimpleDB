package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    
    
    
    
    
    
    
    //My lab implementations:
	private int _tableId;
	private int _ioCostPerPage;
	private HeapFile _dbFile;
	private TupleDesc _tupleDesc;
	private int _numTuples=0;

	private HashMap<String, Integer> minValues;
	private HashMap<String, Integer> _maxValues;
	private HashMap<String, Object> _histograms;
	
    
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
    	_tableId = tableid;
        this._ioCostPerPage = ioCostPerPage;
        _dbFile = (HeapFile)Database.getCatalog().getDatabaseFile(_tableId);
        _tupleDesc = _dbFile.getTupleDesc();
        minValues = new HashMap<String, Integer>();
        _maxValues = new HashMap<String, Integer>();
        _histograms = new HashMap<String, Object>();
        
        Transaction transaction = new Transaction();
        TransactionId tid = transaction.getId();
        DbFileIterator it = _dbFile.iterator(tid);

        
        //Handles exceptions; otherwise gives errors
        try {
        	//Open up the iterator to go through the tuples 
        	it.open(); 
        	
        	//While there are tuples left in the file,
        	//iterate through them, and get their information
        	while (it.hasNext()){ 
        		Tuple tuple = it.next(); 
        		for (int i=0; i<_tupleDesc.numFields(); i++) {
        			String fieldName = _tupleDesc.getFieldName(i);
        			Type fieldType = _tupleDesc.getFieldType(i);

        			if (fieldType.equals(Type.INT_TYPE)) {
        				int value = ((IntField)tuple.getField(i)).getValue();
        				if (!minValues.containsKey(fieldName) || value < minValues.get(fieldName)) {
        					minValues.put(fieldName, value);
        				}
        				if (!_maxValues.containsKey(fieldName) || value > _maxValues.get(fieldName)) {
        					_maxValues.put(fieldName, value);
        				}
        			}
        		}
        	}
        	
        	//Create a new histogram with the information from the first loop
        	for (String key : minValues.keySet()) {
        		IntHistogram newIntHistogram = new IntHistogram(NUM_HIST_BINS, minValues.get(key), _maxValues.get(key));
        		_histograms.put(key, newIntHistogram);
        	}
        	
        	//Restart from the beginning
        	it.rewind();
        	
        	
        	//This time, put the information of the tuples into the histogram
        	while (it.hasNext()){
        		Tuple tuple = it.next();
        		_numTuples++;
        		for (int i=0; i<_tupleDesc.numFields(); i++) {
        			String fieldName = _tupleDesc.getFieldName(i);
        			Type fieldType = _tupleDesc.getFieldType(i);
        			if (fieldType.equals(Type.INT_TYPE)) { 
        				int value = ((IntField)tuple.getField(i)).getValue();
        				IntHistogram intHistogram = (IntHistogram)_histograms.get(fieldName);
        				intHistogram.addValue(value);
        				_histograms.put(fieldName, intHistogram);
        			} 
        			
        			else { 
        				String value = ((StringField)tuple.getField(i)).getValue();
        				if (_histograms.containsKey(fieldName)) {
        					StringHistogram stringHistogram = (StringHistogram)_histograms.get(fieldName);
        					stringHistogram.addValue(value);
        					_histograms.put(fieldName, stringHistogram);
        				} else {
        					StringHistogram stringHistogram = new StringHistogram(NUM_HIST_BINS);
        					stringHistogram.addValue(value);
        					_histograms.put(fieldName, stringHistogram);
        				}
        			}
        		}
        	}

        } 
        
        catch (DbException e) {
        	e.printStackTrace();
        } 
        
        catch (TransactionAbortedException e) {
        	e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
    	double cost = _dbFile.numPages()*_ioCostPerPage;
    	return cost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        int _numTuples = totalTuples();
        return (int)Math.ceil(_numTuples*selectivityFactor);
    }

    
    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     */
  //NOT IMPLEMENTED
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }
    
    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	String fieldName = _tupleDesc.getFieldName(field);
    	Type type = constant.getType();
    	
    	if (type==Type.INT_TYPE) {
    		int value = ((IntField)constant).getValue();
    		IntHistogram histogram = (IntHistogram)_histograms.get(fieldName);
    		return histogram.estimateSelectivity(op, value);
    	} 
    	
    	else {
    		String value = ((StringField)constant).getValue();
    		StringHistogram histogram = (StringHistogram)_histograms.get(fieldName);
    		return histogram.estimateSelectivity(op, value);
    	}
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return _numTuples;
    }

}
