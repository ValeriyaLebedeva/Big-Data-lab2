package laba2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable {
    public String first;
    public String  second;
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

    @Override
    public int compareTo(TextPair c){
        int presentValue=Integer.parseInt(this.first);
        int compareValue=Integer.parseInt(c.first);
        return (Integer.compare(presentValue, compareValue));
    }
    public int hashCode() {
        int a = Integer.parseInt(this.first);
        int b = Integer.parseInt(this.second);
        return a*10+b;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
