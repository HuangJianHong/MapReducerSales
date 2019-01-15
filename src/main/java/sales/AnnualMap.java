package sales;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnnualMap extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //    13,987,1998-01-10,3,999,1,1232.16
        String sales = value.toString();
        String[] split = sales.split(",");
        String year = split[2].substring(0, 4);
        String money = split[6];

        context.write(new IntWritable(Integer.parseInt(year)), new DoubleWritable(Double.parseDouble(money)));

    }


}
