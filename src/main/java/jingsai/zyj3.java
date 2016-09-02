package jingsai;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class zyj3 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(zyj3.class);
		job.setMapperClass(zyjMapper3.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataInfo3.class);
		job.setReducerClass(zyjRecuder3.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataInfo3.class);
		job.setOutputFormatClass(AlphabetOutputFormat3.class);//设置输出格式 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);	
		
	}
	public static class zyjMapper3 extends Mapper<LongWritable, Text, Text, DataInfo3>
	{	
		int i=1;
		
		private Text k = new Text();
		private Text kk = new Text();
		private String post = "POST";
		private int gp;
		private String spe_null = "-";
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DataInfo3>.Context context)
				throws IOException, InterruptedException {
			try {
				
		
			String line = value.toString();
			
			if (line == null || line.isEmpty()) {
				return;
			}
			
			String[] fields = line.split(" ");
			
			if ((fields.length != 10 && fields.length != 9) || fields[0].equals(spe_null)) {
				return;
			}
			
			
			long urlsum = 1;
			String[] shijian = fields[1].split(":");
			k.set(shijian[1] + " " + fields[4]);
			kk.set("0 " + fields[4]);
			if (fields[5].equals(post)) {
				gp = 8;
			}
			else {
				gp = 9;
			}
			DataInfo3 datainfo3 = new DataInfo3(fields[4], Long.parseLong(fields[gp]), urlsum);
			context.write(k, datainfo3);
			context.write(kk, datainfo3);
			} catch (Exception e) {
				return;
			}
		}
		
	}
	
	
	public static class AlphabetOutputFormat3 extends MultipleOutputFormat<Text, DataInfo3>
	{

		@Override
		protected String generateFileNameForKeyValue(Text key, DataInfo3 value, Configuration conf) {
			String name = value.getUrlstring().replaceAll("/", "-");
			String name1 = name.substring(1, name.length());
			return(name1 + ".txt");
		}
	}
		
	
	
	public static class zyjRecuder3 extends Reducer<Text, DataInfo3, Text, DataInfo3>
	{
	
		
		private Text m = new Text();
		private String zero = "0";
		
		@Override
		protected void reduce(Text key, Iterable<DataInfo3> values,
				Reducer<Text, DataInfo3, Text, DataInfo3>.Context context) throws IOException, InterruptedException {
			long jishuurl_sum = 0;
			long jishuresponse_sum = 0;
			for(DataInfo3 d : values)
			{
				jishuresponse_sum += d.getResponse();
				jishuurl_sum += d.getUrl_sum();
			}
			String[] fields = key.toString().split(" ");
			if (fields[0].equals(zero)) {
				m.set(fields[1] + ":" + (jishuresponse_sum/jishuurl_sum));
				DataInfo3 last1_ = new DataInfo3(fields[1], (jishuresponse_sum/jishuurl_sum),0);
				last1_.setMark(1);
				context.write(m, last1_);
			}
			else {
			long sj = Long.parseLong(fields[0]);
			if (sj>12) {
				sj = sj - 12;
			}
			long sjj = sj + 1;
			if (sj == 12) {
				sjj = 0;
			}
			String ssj = String.valueOf(sj);
			String ssjj = String.valueOf(sjj);
			m.set(ssj + ":00-" + ssjj + ":00");
			DataInfo3 last3 = new DataInfo3(fields[1], (jishuresponse_sum/jishuurl_sum), 0);
			context.write(m, last3);
				}
		}
		
	}

}
