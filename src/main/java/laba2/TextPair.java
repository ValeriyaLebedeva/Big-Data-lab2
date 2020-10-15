package laba2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable {
    public String idAirport;
    public String  numTable;
    public TextPair(String idAirport, String numTable){
        this.idAirport =idAirport;
        this.numTable=numTable;
    }
    public void write(DataOutput out) throws IOException {
        out.writeChars(idAirport);
        out.writeChars(numTable);
    }
    public void readFields(DataInput in) throws IOException {
        idAirport = in.readLine();
        numTable = in.readLine();
    }

    @Override
    public int compareTo(TextPair c){
        int presentValue=Integer.parseInt(this.idAirport);
        int compareValue=Integer.parseInt(c.idAirport);
        return (Integer.compare(presentValue, compareValue));
    }
    public int hashCode() {
        int a = Integer.parseInt(this.idAirport);
        int b = Integer.parseInt(this.numTable);
        return a*10+b;
    }
}
