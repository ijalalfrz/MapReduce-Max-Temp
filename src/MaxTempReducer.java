import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //looping grup dari temperatur
        //inisialisasi awal maximum temperatur
        int maxVal = Integer.MIN_VALUE;
        for(IntWritable val : values){
            //cek yang mana paling besar antar value iterasi dan variabel max
            maxVal = Math.max(maxVal,val.get());
        }
        //output dikembalikan ke context
        //contoh: {1901, 32}
        context.write(key,new IntWritable(maxVal));
    }
}
