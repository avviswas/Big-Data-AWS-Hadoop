import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.*;

 
public class CalMap2 extends Mapper<Object, Text, DoubleWritable, Splitter>
{
	private String[] first;
    private String[] tokens;
    private DoubleWritable relativefreq = new DoubleWritable();
 
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		StringTokenizer st = new StringTokenizer(value.toString(), "\n");

		while (st.hasMoreTokens())
		{
			tokens = st.nextToken().toString().split("\t");
      	    first = tokens[0].toString().split(" ");
            Splitter twoWords = new Splitter(first[0], first[1]);
      	    relativefreq.set(Double.parseDouble(tokens[1].trim()));
			  
            if(relativefreq == null)
                continue;
			  
            context.write(relativefreq, twoWords);
		}
	}
}