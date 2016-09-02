package jingsai;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/*
public class DataInfo5 implements Writable{
private String urlstring;
private long[] url_data_sum = new long[15];
private int mark;
	
	
	public DataInfo5(){}
	
	public DataInfo5(String urlstring,long[] url_data_sum)
	{
		this.urlstring = urlstring;
		for(int i = 0; i < 15; i++ )
		{
			this.url_data_sum[i] = url_data_sum[i];
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(urlstring);

		for(int i = 0; i < 15; i++ )
		{
			out.writeLong(url_data_sum[i]);
		}
		
	}

	public void readFields(DataInput in) throws IOException {
		this.urlstring = in.readUTF();

		for(int i = 0; i < 15; i++ )
		{
			this.url_data_sum[i] = in.readLong();
		}
		
	}
	
	public String toString()
	{
		if (mark == 1) {
			return "";
		}
		else {
			
			return urlstring + ":" + url_data_sum[0] + "  " + url_data_sum[1] + "  " + url_data_sum[2] + "  " + url_data_sum[3] + "  " + url_data_sum[4] + "  " + url_data_sum[5] + "  " + url_data_sum[6] + "  " + url_data_sum[7] + "  " + url_data_sum[8] + "  " + url_data_sum[9] + "  " + url_data_sum[10] + "  " + url_data_sum[11] + "  " + url_data_sum[12] + "  " + url_data_sum[13] + "  " + url_data_sum[14]; 
		}
		
	}

	public String getUrlstring() {
		return urlstring;
	}

	public void setUrlstring(String urlstring) {
		this.urlstring = urlstring;
	}

	public long[] getUrl_data_sum() {
		return url_data_sum;
	}

	public void setUrl_data_sum(long[] url_data_sum) {
		this.url_data_sum = url_data_sum;
	}

	public int getMark() {
		return mark;
	}

	public void setMark(int mark) {
		this.mark = mark;
	}
	*/
//以上全体测试时再解封

public class DataInfo5 implements Writable{
private String urlstring;
private long[] url_data_sum = new long[4];
private int mark;
	
	
	public DataInfo5(){}
	
	public DataInfo5(String urlstring,long[] url_data_sum)
	{
		this.urlstring = urlstring;
		for(int i = 0; i < 4; i++ )
		{
			this.url_data_sum[i] = url_data_sum[i];
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(urlstring);

		for(int i = 0; i < 4; i++ )
		{
			out.writeLong(url_data_sum[i]);
		}
		
	}

	public void readFields(DataInput in) throws IOException {
		this.urlstring = in.readUTF();

		for(int i = 0; i < 4; i++ )
		{
			this.url_data_sum[i] = in.readLong();
		}
		
	}
	
	public String toString()
	{
		if (mark == 1) {
			return "";
		}
		else {
			
			return urlstring + ":" + url_data_sum[3]; 
		}
		
	}

	public String getUrlstring() {
		return urlstring;
	}

	public void setUrlstring(String urlstring) {
		this.urlstring = urlstring;
	}

	public long[] getUrl_data_sum() {
		return url_data_sum;
	}

	public void setUrl_data_sum(long[] url_data_sum) {
		this.url_data_sum = url_data_sum;
	}

	public int getMark() {
		return mark;
	}

	public void setMark(int mark) {
		this.mark = mark;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
}
