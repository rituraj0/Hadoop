package sales;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends
		Mapper<LongWritable, Text, Text, FloatWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] splits = line.split("\t");

		if (splits.length != 6) {
			return;
		}
		
		/*
		     String tp = splits[3];        
        
        if( tp.equals("Books")  || tp.equals("Computers") )
        {            
        }
        else
        {
            return;
        }

        context.write(new Text(splits[3]),
                new FloatWritable(Float.valueOf(splits[4])));//to just count  sell of item Books and Computers
	    */			

		context.write(new Text(splits[2]),
				new FloatWritable(Float.valueOf(splits[4])));

	}
}