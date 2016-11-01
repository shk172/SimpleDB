package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

	private TDItem[] fields;
	
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator()  {
    	Iterator<TDItem> it = new Iterator<TDItem>() {
	    	int i = 0;
	    	
	    	public boolean hasNext(){
	    		return i < fields.length;

	    	}
	    	public TDItem next(){
	    		return fields[i++];
	    	}
    	};
    	return it;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr){
    	fields = new TDItem[typeAr.length];
    	for (int i = 0; i < typeAr.length; i++) {
    		fields[i] = new TDItem(typeAr[i], fieldAr[i]);
    	}
    		
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        if(typeAr.length <= 0){
        	throw new IllegalArgumentException("There must be at least one entry for Type");
        }
        
        else{
        	fields = new TDItem[typeAr.length];
        	for (int i = 0; i < typeAr.length; i++) {
        		fields[i] = new TDItem(typeAr[i], null);
        	}
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return fields.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i > fields.length || i < 0) {
        	throw new NoSuchElementException("Index must be within bounds of the array");
        }
    	return fields[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i > fields.length || i < 0) {
        	throw new NoSuchElementException("Index must be within bounds of the array");
        }
    	return fields[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	for (int i = 0; i < fields.length; i++) {
    		if (fields[i].fieldName != null && fields[i].fieldName.equals(name)) {;
    			return i;
        	}
        }
        throw new NoSuchElementException("name not in field array");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int sum = 0;
        for (int i = 0; i < fields.length; i++) {
        	sum += fields[i].fieldType.getLen();
        }
        return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int td1l = td1.fields.length;
    	String[] tdnames = new String[td1l + td2.fields.length];
    	Type[] tdtypes = new Type[td1l + td2.fields.length];
    	for (int i = 0; i < td1l; i++) {
    		tdnames[i] = td1.fields[i].fieldName;
    		tdtypes[i] = td1.fields[i].fieldType;
    	}
    	for (int j = 0; j < td2.fields.length; j++) {
    		tdnames[j + td1l] = td2.fields[j].fieldName;
    		tdtypes[j + td1l] = td2.fields[j].fieldType;
    	}
    		TupleDesc td;
				td = new TupleDesc(tdtypes, tdnames);
				return td;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)) {
        	return false;
        }
        if (fields.length != ((TupleDesc) o).fields.length) {
        	return false;
        }
        for (int i = 0; i < fields.length; i++) {
        	if (fields[i].fieldType != ((TupleDesc) o).fields[i].fieldType) {
        		System.out.println("me: " + fields[i] + " object: " + ((TupleDesc) o).fields[i]);
        		return false;
        	}
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	StringBuilder fieldDesc = new StringBuilder();
        for (TDItem item : fields) {
        	fieldDesc.append(item.fieldType);
        	fieldDesc.append("(");
        	fieldDesc.append(item.fieldName);
        	fieldDesc.append("), ");
        }
        return fieldDesc.toString();
    }
}
