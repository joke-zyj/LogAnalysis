package jingsai;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataInfo implements Writable{
	private long jishu200;
	private long jishu404;
	private long jishu500;
	private int mark;
	
	public DataInfo(){}	
	
	public DataInfo(long jishu200, long jishu404, long jishu500)
	{
		this.jishu200 = jishu200;
		this.jishu404 = jishu404;
		this.jishu500 = jishu500;
	}

	public void write(DataOutput out) throws IOException {
	out.writeLong(jishu200);
	out.writeLong(jishu404);
	out.writeLong(jishu500);	
	}

	public void readFields(DataInput in) throws IOException {
		this.jishu200 = in.readLong();
		this.jishu404 = in.readLong();
		this.jishu500 = in.readLong();
	}
	public String toString()
	{
		if (mark == 1) {
			return "";
		}
		else {
		return "200:" + jishu200 + " 404:" + jishu404 + " 500:" + jishu500;
		}
	}

	public long getJishu200() {
		return jishu200;
	}

	public void setJishu200(long jishu200) {
		this.jishu200 = jishu200;
	}

	public long getJishu404() {
		return jishu404;
	}

	public void setJishu404(long jishu404) {
		this.jishu404 = jishu404;
	}

	public long getJishu500() {
		return jishu500;
	}

	public void setJishu500(long jishu500) {
		this.jishu500 = jishu500;
	}
	
	public int getMark() {
		return mark;
	}

	public void setMark(int mark) {
		this.mark = mark;
	}


}
