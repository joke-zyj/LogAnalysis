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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
public class zyj4 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(zyj4.class);
		job.setMapperClass(zyjMapper4.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataInfo4.class);
		job.setReducerClass(zyjRecuder4.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataInfo5.class);
		job.setOutputFormatClass(AlphabetOutputFormat4.class);//设置输出格式 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);	
		

	}
	
	public static class zyjMapper4 extends Mapper<LongWritable, Text, Text, DataInfo4>
	{
		private Text k = new Text();
		private Text kk = new  Text();
		private String spe_null = "-";
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DataInfo4>.Context context)
				throws IOException, InterruptedException {
			
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
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			Path path = inputSplit.getPath();
			String name = path.getName();
			//String[] date = name.split(".");
			//String[] date1 = date[0].split("-");
			long date_num = Long.parseLong(name.substring(8, 10));
			//long date_num = Long.parseLong(date1[2]);
			DataInfo4 dataInfo4 = new DataInfo4(date_num, urlsum);
			context.write(k, dataInfo4);
			context.write(kk, dataInfo4);
		}

		
		}
		
	
	public static class AlphabetOutputFormat4 extends MultipleOutputFormat<Text, DataInfo5>
	{

		@Override
		protected String generateFileNameForKeyValue(Text key, DataInfo5 value, Configuration conf) {
			String name = value.getUrlstring().replaceAll("/", "-");
			String name1 = name.substring(1, name.length());
			return(name1 + ".txt");
		}
		
	}
	
	public static class zyjRecuder4 extends Reducer<Text, DataInfo4, Text, DataInfo5>
	{
		private Text m = new Text();
		private long[] jishuurl_sum = new long[15];
		private String zero = "0";
		@Override
		protected void reduce(Text key, Iterable<DataInfo4> values,
				Reducer<Text, DataInfo4, Text, DataInfo5>.Context context) throws IOException, InterruptedException {
			for(int i =0; i < 15; i++)
			{
				jishuurl_sum[i] = 0;
			}
			for(DataInfo4 d : values)
			{
				jishuurl_sum[(int) d.getDate() - 8] += d.getUrl_sum();
			}
			String[] fields = key.toString().split(" ");
			if (fields[0].equals(zero)) {
				m.set(fields[1] + ":" + jishuurl_sum[0] + "  " + jishuurl_sum[1] + "  " + jishuurl_sum[2] + "  " + jishuurl_sum[3] + "  " + jishuurl_sum[4] + "  " + jishuurl_sum[5] + "  " + jishuurl_sum[6] + "  " + jishuurl_sum[7] + "  " + jishuurl_sum[8] + "  " + jishuurl_sum[9] + "  " + jishuurl_sum[10] + "  " + jishuurl_sum[11] + "  " + jishuurl_sum[12] + "  " + jishuurl_sum[13] + "  " + jishuurl_sum[14]);
				DataInfo5 last1_ = new DataInfo5(fields[1], jishuurl_sum);
				last1_.setMark(1);
				context.write(m, last1_);
			}
			else{
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
			DataInfo5 last5 = new DataInfo5(fields[1],jishuurl_sum);
			context.write(m, last5);
			}
		}
		
	}
	}
*/
//以上全体测试的时候再解封
public class zyj4 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(zyj4.class);
		job.setMapperClass(zyjMapper4.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataInfo4.class);
		job.setReducerClass(zyjRecuder4.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataInfo5.class);
		job.setOutputFormatClass(AlphabetOutputFormat4.class);//设置输出格式 
		if(args[0].charAt(args[0].length()-1)=='/')
		{
			FileInputFormat.setInputPaths(job, new Path(args[0] + "2015-09-15.log"));
			FileInputFormat.addInputPath(job, new Path(args[0] + "2015-09-16.log"));
			FileInputFormat.addInputPath(job, new Path(args[0] + "2015-09-22.log"));
		}
		else
		{
			FileInputFormat.setInputPaths(job, new Path(args[0] + "/2015-09-15.log"));
			FileInputFormat.addInputPath(job, new Path(args[0] + "/2015-09-16.log"));
			FileInputFormat.addInputPath(job, new Path(args[0] + "/2015-09-22.log"));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);	
		

	}
	
	public static class zyjMapper4 extends Mapper<LongWritable, Text, Text, DataInfo4>
	{
		private Text k = new Text();
		private Text kk = new  Text();
		private String spe_null = "-";
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DataInfo4>.Context context)
				throws IOException, InterruptedException {
			
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
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			Path path = inputSplit.getPath();
			String name = path.getName();
			//String[] date = name.split(".");
			//String[] date1 = date[0].split("-");
			long date_num = Long.parseLong(name.substring(8, 10));
			//long date_num = Long.parseLong(date1[2]);
			DataInfo4 dataInfo4 = new DataInfo4(date_num, urlsum);
			context.write(k, dataInfo4);
			context.write(kk, dataInfo4);
		}

		
		}
		
	
	public static class AlphabetOutputFormat4 extends MultipleOutputFormat<Text, DataInfo5>
	{

		@Override
		protected String generateFileNameForKeyValue(Text key, DataInfo5 value, Configuration conf) {
			String name = value.getUrlstring().replaceAll("/", "-");
			String name1 = name.substring(1, name.length());
			return(name1 + ".txt");
		}
		
	}
	
	public static class zyjRecuder4 extends Reducer<Text, DataInfo4, Text, DataInfo5>
	{
		private Text m = new Text();
		private long[] jishuurl_sum = new long[4];
		private String zero = "0";
		@Override
		protected void reduce(Text key, Iterable<DataInfo4> values,
				Reducer<Text, DataInfo4, Text, DataInfo5>.Context context) throws IOException, InterruptedException {
			for(int i =0; i < 4; i++)
			{
				jishuurl_sum[i] = 0;
			}
			for(DataInfo4 d : values)
			{
				if(((int) d.getDate()) == 15)
				{
					jishuurl_sum[0] += d.getUrl_sum(); 
				}
				if(((int) d.getDate()) == 16)
				{
					jishuurl_sum[1] += d.getUrl_sum(); 
				}
				if(((int) d.getDate()) == 22)
				{
					jishuurl_sum[2] += d.getUrl_sum(); 
				}
				//jishuurl_sum[(int) d.getDate() - 8] += d.getUrl_sum();
			}

			String[] fields = key.toString().split(" ");
			if (fields[0].equals(zero)) {
				if (jishuurl_sum[0] == 0) {
					jishuurl_sum[3] = 0;
				}
				else
				{
					jishuurl_sum[3] = (long)(((double)jishuurl_sum[1])/((double)jishuurl_sum[0])*((double)jishuurl_sum[2]));
				}
				m.set(fields[1] + ":" + jishuurl_sum[3]);
				DataInfo5 last1_ = new DataInfo5(fields[1], jishuurl_sum);
				last1_.setMark(1);
				context.write(m, last1_);
			}
			else{
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
			if (jishuurl_sum[0] == 0) {
				jishuurl_sum[3] = 0;
			}
			else
			{
				jishuurl_sum[3] = (long)(((double)jishuurl_sum[1])/((double)jishuurl_sum[0])*((double)jishuurl_sum[2]));
			}
			DataInfo5 last5 = new DataInfo5(fields[1],jishuurl_sum);
			context.write(m, last5);
			}
		}
		
	}
	}














