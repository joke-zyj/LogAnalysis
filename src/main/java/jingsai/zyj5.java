package jingsai;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class zyj5 {

	public static void main(String[] args) {
		

	}
	public static class zyjMapper5 extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text k = new Text();
		private Text kk = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(" ");
			if (fields.length == 2) {
				k.set(fields[0]);
			}
			if (fields.length != 2) {
				k.set("0");
			}
			kk.set(fields[1]);
			context.write(k, kk);
			
		}
		
	}
	
	public class zyjReducer5 extends Reducer<Text, Text, Text, Text>
	{
		private Text m = new Text();
		private Text mm = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String jishuurl_sun_type = "";
			for(Text d : values)
			{
				jishuurl_sun_type += (d.toString()+" ");
			}
			m.set(key.toString());
			mm.set(jishuurl_sun_type);
			context.write(m, mm);
		}
		
	}
	
	
	
	
	
	
	
	
	
	
	
	

}
