package BigData;

import java.io.*;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class PairWritable implements WritableComparable<PairWritable>{
    private int num1;
    private int num2;

    public PairWritable(){
    }

    public void write(DataOutput out) throws IOException{
        out.writeInt(num1);
        out.writeInt(num2);
    }

    public void readFields(DataInput in) throws IOException{
        num1 = in.readInt();
        num2 = in.readInt();
    }

    public int getNum1(){
        return num1;
    }

    public int getNum2(){
        return num2;
    }

    public void set(int num1, int num2){
        this.num1 = num1;
        this.num2 = num2;
    }

    @Override
    public int compareTo(PairWritable o) {
        return this.num1-o.num1;
    }
}
