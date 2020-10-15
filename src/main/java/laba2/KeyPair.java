package laba2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyPair implements WritableComparable<KeyPair> {
    public Text idAirport;
    public Text mark;
    public KeyPair(Text idAirport, Text mark){
        this.idAirport = idAirport;
        this.mark = mark;
    }
    public void write(DataOutput out) throws IOException {
        out.writeChars(idAirport.toString());
        out.writeChars(mark.toString());
    }
    public void readFields(DataInput in) throws IOException {
        idAirport = new Text(in.readLine());
        mark = new Text(in.readLine());
    }
    public int compareTo(KeyPair c){
        int presentValue=Integer.parseInt(this.idAirport.toString());
        int compareValue=Integer.parseInt(c.idAirport.toString());
        return (Integer.compare(presentValue, compareValue));
    }
    public int hashCode() {
        int a = Integer.parseInt(this.idAirport.toString());
        int b = Integer.parseInt(this.mark.toString());
        return a*10+b;
    }
}
