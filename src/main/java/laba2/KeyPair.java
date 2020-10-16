package laba2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyPair implements WritableComparable<KeyPair> {
    private Text idAirport;
    private Text mark;

    public KeyPair() {
        super();
    }

    public KeyPair(Text idAirport, Text mark){
        this.idAirport = idAirport;
        this.mark = mark;
    }
    public void write(DataOutput out) throws IOException {
        out.writeUTF(idAirport.toString());
        out.writeUTF(mark.toString());
    }
    public void readFields(DataInput in) throws IOException {
        idAirport = new Text(in.readUTF());
        mark = new Text(in.readUTF());
    }
    public int compareTo(KeyPair c){
        int r1 =  this.idAirport.toString().compareTo(c.idAirport.toString());
        if (r1 == 0 ){
            return this.mark.toString().compareTo(c.mark.toString());
        } else {
            return r1;
        }
    }

    public Text getIdAirport() {
        return this.idAirport;
    }
    public Text getMark() {
        return this.idAirport;
    }
}
