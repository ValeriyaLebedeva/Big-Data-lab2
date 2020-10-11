package laba2;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TextPair implements Writable {
    public String first;
    public String second;
    public TextPair(){
        this.first=first;
        this.second=second;
    }
    public void write(DataOutput out) throws IOException {
        out.writeChars(first);
        out.writeChars(second);
    }
    public void readFields(DataInput in) throws IOException {
        a = in.readInt();
        b = in.readInt();
    }
    public String toString() {
        return Integer.toString(a) + ", " + Integer.toString(b)
    }
}


public class TextPair implements WritableComparable {
    public int first;
    public int second;
    public TextPair(){
        this.first=first;
        this.second=second;
    }
    public void write(DataOutput out) throws IOException {
        out.writeint(a);
        out.writeint(b);
    }
    public void readFields(DataInput in) throws IOException {
        a = in.readint();
        b = in.readint();
    }
    public int CompareTo(add c){
        int presentValue=this.value;
        int CompareValue=c.value;
        return (presentValue < CompareValue ? -1 : (presentValue==CompareValue ? 0 : 1));
    }
    public int hashCode() {
        return Integer.IntToIntBits(a)^ Integer.IntToIntBits(b);
    }
}
