package sales;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AnnualTotalReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double totalMoney = 0.0;
        int totalSales = 0;

        for (DoubleWritable v : values) {
            totalMoney += v.get();
            totalSales++;
        }

        context.write(key, new Text("sales: " + totalSales + " Money： " + totalMoney));
    }
}
