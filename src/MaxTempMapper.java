import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper extends Mapper<LongWritable, Text , Text, IntWritable> {
    //tanda data miss
    private static final int MISSING = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //ambil record
        String line = value.toString();

        //ambil tahun dari karakter ke 15 sampai 19 , (4 karakter)
        String year = line.substring(15,19);

        //ambil temperature udara

        int airTemp;
        //cek apakah suhunya positif/negatif
        if(line.charAt(87)=='+'){
            airTemp = Integer.parseInt(line.substring(88, 92));
        }else{
            //negatif val
            airTemp = Integer.parseInt(line.substring(87,92));
        }

        //ambil dan cek data kualitas udara maka data yang akan dilanjutkan yang memiliki kualitas saja
        //masuk dalam regex [01459]
        String quality = line.substring(92,93);
        if(airTemp != MISSING && quality.matches("[01459]")){
            context.write(new Text(year),new IntWritable(airTemp));
        }
    }
}
