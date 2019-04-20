import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int maxVal = Integer.MIN_VALUE;
        for(IntWritable val : values){
            maxVal = Math.max(maxVal,val.get());
        }

        context.write(key,new IntWritable(maxVal));
    }
}
