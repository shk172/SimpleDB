package simpledb;
import static java.lang.Math.*;
/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private int[] _buckets;
    private int _numBuckets;
    private int _average;
    private int _totalValue;
    private int _min;
    private int _max;
    
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	//Initialize the buckets
    	_buckets = new int[buckets];
        _numBuckets = buckets;
        
        //Find the average value across the buckets
        _min = min;
        _max = max;
        _average= (int) Math.ceil((double) (_max - _min + 1)/buckets);
        
        //Global to keep track of the total number of values
        _totalValue = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	//Find the position of the bucket to increment
    	int bucketPos = (v - _min)/_average;
    	
    	//And increment it, and add to the total value.
        _buckets[bucketPos]++;
        _totalValue++;
    }

    
    
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	if(op == Predicate.Op.EQUALS || op == Predicate.Op.LIKE)
            return equals(v);
    	else if(op == Predicate.Op.GREATER_THAN)
        	return CheckGreaterThan(v);
    	
    	else if (op == Predicate.Op.LESS_THAN)
            return CheckLessThan(v);
    	
    	else if(op == Predicate.Op.LESS_THAN_OR_EQ)
            return min(1.0, equals(v) + CheckLessThan(v));
    	
    	else if(op == Predicate.Op.GREATER_THAN_OR_EQ)
            return min(1.0, equals(v) + CheckGreaterThan(v));
    	
    	else if(op == Predicate.Op.NOT_EQUALS)
            return 1.0 - equals(v);
    	
    	else
            return -1.0; 
    }
    
    //Extra helper function for organizational purpose:
    //to be used when estimating the value to be greater
    //than the given value.
    private double CheckGreaterThan(int v){
    	int _bucket = findBucket(v);
        int bucket_f;
        int rightb;
        int height;
      
        if (_bucket <= 0) {
            rightb = 0;
            bucket_f = 0;
            height = 0;
        }
        
        else if (_bucket >= _numBuckets) {
	  	    rightb = _numBuckets;
	  	    bucket_f = 0;
	        height = 0;
        }
        
        else  {
		    rightb = _bucket+1;
	        bucket_f = -1;
	        height = _buckets[_bucket];
        }
        
        double selectivity = 0.0;
        
        if (bucket_f == -1) {
        	bucket_f = ((rightb*_average)+_min-v)/_average;
        }
        selectivity = height * bucket_f;
        if (rightb >= _numBuckets)
        	return selectivity/_totalValue;
        for (int i = rightb; i < _numBuckets; i++) {
        	selectivity += _buckets[i];
        }
        return selectivity/_totalValue;
    }
    
    
	//Extra helper function for organizational purpose:
    //to be used when estimating the value to be less
    //than the given value.
    private double CheckLessThan(int v){
        int _bucket = findBucket(v);
        int bucket_f;
        int leftb;
        int height;
      
        if (_bucket <= 0) {
       	    leftb = -1;
            bucket_f = 0;
            height = 0;
        }
        
        else if (_bucket >= _numBuckets) {
        	leftb = _numBuckets-1; 
            bucket_f = 0;
            height = 0;
        }
        
        else  {
            leftb = _bucket-1;
            bucket_f = -1;
            height = _buckets[_bucket];
        }
        
        double selectivity = 0.0;
        
        if (bucket_f == -1) {
        	bucket_f = (v-(leftb*_average)+_min)/_average;
        }
        selectivity = height * bucket_f;
        if (leftb < 0)
        	return selectivity/_totalValue;
        for (int i = leftb; i >= 0; i--) {
        	selectivity += _buckets[i];
        }
        return selectivity/_totalValue;
    }
    
    //Finds the bucket
    private int findBucket(int v) {
        int bucket = (v - _min)/_average;
        if (bucket < 0) {
        	return -1;
        }
        if (bucket >= _numBuckets) {
        	return _numBuckets;
        }
        return bucket;
    }

    //Helper function to be used when the values are equal
    private double equals(int v) {
        int bucket = findBucket(v);
        if (bucket < 0)
        	return 0.0;
        if (bucket >= _numBuckets)
        	return 0.0;
        int height = _buckets[bucket];
        return (double) ((double) height/_average)/_totalValue;
    }
    
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
    	double toRet = 0;
    	int denom = (_totalValue*_totalValue);
    	for(int i = 0; i < _numBuckets; i++) {
    		int toSquare = _buckets[i];
    		toRet += (toSquare*toSquare);
    	}
    	return (toRet/denom);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	String ret = "";
    	for (int i = 0; i < _numBuckets; i++) {
    		ret += "bucket " + i + ": ";
    		for (int j = 0; j < _buckets[i]; j++) {
    			ret += "|";
    		}
    		ret += "\n";
    	}
        return ret;
    }
}
