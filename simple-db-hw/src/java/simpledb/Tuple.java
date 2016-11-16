package simpledb;

import java.io.Serializable;
//import java.util.Arrays;
import java.util.Iterator;

import simpledb.TupleDesc.TDItem;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tupleDescription;
    private Field[] fieldArray;
    private RecordId rid;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {

    	//Store the tuple description
    	tupleDescription = td;

    	//We have to call the iterator to access the fields of the description because it is private
    	Iterator<TDItem> it = tupleDescription.iterator();

    	//Create new field for each of the entries in the description, and store
    	//it in the array of fields in the tuples.
    	fieldArray = new Field[td.numFields()];
    	for(int i = 0; i < td.numFields(); i++){
    		// make sure there's another tuple
    		if (it.hasNext()) {
    			// add it to the fieldArray
    			fieldArray[i] = it.next();
    		}
    	}
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDescription;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        fieldArray[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fieldArray[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
    	StringBuilder ret = new StringBuilder();
      for (int i = 0; i < fieldArray.length - 1; i++) {
          ret.append(fieldArray + " ");
      }
      ret.append(fieldArray[fieldArray.length - 1] + "\n");
      return ret.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
      Iterator<Field> it = new Iterator<Field>() {
        int i = 0;

        public boolean hasNext(){
          return i < fieldArray.length;

        }
        public Field next(){
          return fieldArray[i++];
        }
      };
      return it;
    }

    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
    	//Store the tuple description
    	tupleDescription = td;

    	//We have to call the iterator to access the fields of the description because it is private
    	Iterator<TDItem> it = tupleDescription.iterator();

    	//Create new field for each of the entries in the description, and store
    	//it in the array of fields in the tuples.
    	fieldArray = new Field[td.numFields()];
    	for(int i = 0; i < td.numFields(); i++){
    		if (it.hasNext()) {
    			fieldArray[i] = it.next();
    		}
    	}
    }
}
