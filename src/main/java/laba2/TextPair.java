package laba2;


import com.google.common.primitives.Chars;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


//public class TextPair implements Writable {
//    public String first;
//    public String second;
//    public TextPair(){
//        this.first=first;
//        this.second=second;
//    }
//    public void write(DataOutput out) throws IOException {
//        out.writeChars(first);
//        out.writeChars(second);
//    }
//    public void readFields(DataInput in) throws IOException {
//        first = in.readLine();
//        second = in.readLine();
//    }
//    public String toString() {
//        return first + ", " + second;
//    }
//}


public class TextPair implements WritableComparable {
    public String first;
    public String second;
    public TextPair(String first, String second){
        this.first=first;
        this.second=second;
    }
    public void write(DataOutput out) throws IOException {
        out.writeChars(first);
        out.writeChars(second);
    }
    public void readFields(DataInput in) throws IOException {
        first = in.readLine();
        second = in.readLine();
    }
    public int CompareTo(TextPair c){
        int presentValue=this.first);
        int compareValue=c.first;
        return (intpresentValue < compareValue ? -1 : (presentValue==compareValue ? 0 : 1));
    }
    public int hashCode() {
        return Integer.IntToIntBits(a)^ Integer.IntToIntBits(b);
    }
}
