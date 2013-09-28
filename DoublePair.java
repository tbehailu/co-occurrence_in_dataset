/*
 * CS 61C Fall 2013 Project 1
 *
 * DoublePair.java is a class which stores two doubles and 
 * implements the Writable interface. It can be used as a 
 * custom value for Hadoop. To use this as a key, you can
 * choose to implement the WritableComparable interface,
 * although that is not necessary for credit.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DoublePair implements Writable {
    // Declare any variables here

    double doub1;
    double doub2;
    
    /**
     * Constructs a DoublePair with both doubles set to zero.
     */
    public DoublePair() {
        doub1 = 0;
        doub2 = 0;

    }

    /**
     * Constructs a DoublePair containing double1 and double2.
     */ 
    public DoublePair(double double1, double double2) {
        doub1 = double1;
        doub2 = double2;

    }

    /**
     * Returns the value of the first double.
     */
    public double getDouble1() {
        return doub1;
        
    }

    /**
     * Returns the value of the second double.
     */
    public double getDouble2() {
        return doub2;
        
    }

    /**
     * Sets the first double to val.
     */
    public void setDouble1(double val) {
        this.doub1 = val;

    }

    /**
     * Sets the second double to val.
     */
    public void setDouble2(double val) {
        this.doub2 = val;

    }

    /**
     * write() is required for implementing Writable.
     */
    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.doub1);
        out.writeDouble(this.doub2);

    }

    /**
     * readFields() is required for implementing Writable.
     */
    public void readFields(DataInput in) throws IOException {
        doub1 = in.readDouble();
        doub2 = in.readDouble();

    }
    
    public static void main(String [] args) {
        DoublePair test = new DoublePair();
        System.out.println("double1 (should be 0): " + test.doub1 + " double2 (should be 0): " + test.doub2);
    }

}
