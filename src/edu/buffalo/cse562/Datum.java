package edu.buffalo.cse562;

import java.io.Serializable;
import java.sql.Date;
import java.text.SimpleDateFormat;

public interface Datum extends Comparable<Datum> 
{  	
	class Int implements Datum, Serializable
	{
		private static final long serialVersionUID = 1L;
		Integer value = null;
		
		public int hashCode() {
	        return this.value.hashCode();
	    }
		
		public Int(String value) 
		{
			if(!value.equals("") && value != null)
				this.value = Integer.parseInt(value);
		}
		public Int(Integer value) 
		{
			this.value = value;
		}
		public Int(Int v) {
			this.value = v.value;
		}
		public Integer getValue() {			
			return value;
		}
		public void setValue(int value) {
			this.value = value;
		}
		public String toString() {
			return String.valueOf(value);
		}
		public int compareTo(Flt o) 
		{
			try
			{
				if((float)this.value > o.getValue())
					return 1;
				else if((float)this.value < o.getValue())
					return -1;	
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			return 0;
		}
		public int compareTo(Int o) 
		{
			try
			{
				if(this.value > o.getValue())
					return 1;
				else if(this.value < o.getValue())
					return -1;	
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			return 0;
		}
		public int equals(Flt o) 
		{
			try
			{
				if((float)this.value > o.getValue())
					return 1;
				else if((float)this.value < o.getValue())
					return -1;	
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			return 0;
		}
		public int equals(Int o) 
		{
			try
			{
				if(this.value > o.getValue())
					return 1;
				else if(this.value < o.getValue())
					return -1;	
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			return 0;
		}
		
		public Flt toFlt(Datum o)
		{
			if(o instanceof Int)
				return new Flt(((Int) o).getValue().floatValue());
			else if(o instanceof Flt)
				return (Flt) o;
			return null;
		}
		
		public Integer add(Int o)
		{
			return this.getValue() + ((Int) o).getValue();
		}
		public Float add(Flt o)
		{
			return (((Int) this).toFlt(this).getValue() + o.getValue());
		}
		
		public Integer subtract(Int o)
		{
			return this.getValue() - ((Int) o).getValue();
		}
		public Float subtract(Flt o)
		{
			return (((Int) this).toFlt(this).getValue() - o.getValue());
		}
		
		public Integer multiply(Int o)
		{
			return this.getValue()* ((Int) o).getValue();
		}
		public Float multiply(Flt o)
		{
			return (((Int) this).toFlt(this).getValue() * o.getValue());
		}
		
		public Float divide(Int o)
		{
			return (float) (this.getValue() / ((Int) o).getValue());
		}
		public Float divide(Flt o)
		{
			return (float) (((Int) this).toFlt(this).getValue() / o.getValue());
		}
		@Override
		public int compareTo(Datum o) {
			if(o instanceof Flt)
				return compareTo((Flt) o);
			else
				return compareTo((Int) o);
		}
	}
	
	class Flt implements Datum, Serializable
	{
		private static final long serialVersionUID = 1L;
		Float value = null;
		
		public int hashCode() {
	        return this.value.hashCode();
	    }
		
		public Flt(String value) {
			if(value != null && !value.equals(""))
				this.value = Float.parseFloat(value);
		}
		public Flt(Float value) {
			this.value = value;
		}
		public Flt(Integer value) {
			this.value = (float)value;
		}
		public Flt(Flt v) {
			this.value = v.value;
		}
		public Float getValue() {
			return value;
		}
		public void setValue(Float value) {
			this.value = value;
		}
		public String toString() {
			return String.valueOf(value);
		}
		public int compareTo(Flt o) 
		{
			try
			{
				if(this.value > o.getValue())
					return 1;
				else if(this.value < o.getValue())
					return -1;	
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			return 0;
		}
		public int compareTo(Int o) 
		{
			try
			{
				if(this.value > (float)o.getValue())
					return 1;
				else if(this.value < (float)o.getValue())
					return -1;	
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			return 0;
		}
		public int equals(Flt o) 
		{
			try
			{
				if(this.value > o.getValue())
					return 1;
				else if(this.value < o.getValue())
					return -1;	
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			return 0;
		}
		public int equals(Int o) 
		{
			try
			{
				if(this.value > (float)o.getValue())
					return 1;
				else if(this.value < (float)o.getValue())
					return -1;	
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			return 0;
		}
		
		public Float add(Flt o)
		{
			return (Float)(this.getValue() + o.getValue());
		}
		public Float add(Int o)
		{
			return (Float)(this.getValue() + ((Int)o).toFlt(o).getValue());
		}
		
		public Float subtract(Flt o)
		{
			return (Float) (this.getValue() - ((Flt)o).getValue());
		}
		public Float subtract(Int o)
		{
			return (Float) (this.getValue() - ((Int)o).toFlt(o).getValue());
		}
		
		public Float multiply(Flt o)
		{
			return (Float)(this.getValue() * ((Flt)o).getValue());
		}
		public Float multiply(Int o)
		{
			return (Float)(this.getValue() * ((Int)o).toFlt(o).getValue());
		}
		
		public Float divide(Flt o)
		{
			return (Float) (this.getValue() / ((Flt)o).getValue());
		}
		public Float divide(Int o)
		{
			return (Float) (this.getValue() / ((Int)o).toFlt(o).getValue());
		}
		@Override
		public int compareTo(Datum o) {
			if(o instanceof Flt)
				return compareTo((Flt) o);
			else
				return compareTo((Int) o);
		}
		public Flt toFlt(Datum o)
		{
			if(o instanceof Int)
				return new Flt(((Int) o).getValue().floatValue());
			else if(o instanceof Flt)
				return (Flt) o;
			return null;
		}
	}
	
	class Str implements Datum, Serializable
	{
		private static final long serialVersionUID = 1L;
		String value = null;
		
		public int hashCode() {
	        return this.value.toUpperCase().hashCode();
	    }		
		public Str(String value) {
			if(!value.equals("") && value != null)
				this.value = value;
		}
		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}
		public String toString() {
			return value;
		}
		@Override
		public int compareTo(Datum o) 
		{
			return this.value.compareTo(((Str) o).getValue());			
		}
		public int equals(Datum o) 
		{
			return this.value.toLowerCase().compareTo(((Str) o).getValue().toLowerCase());			
		}
	}
	
	class Bool implements Datum, Serializable
	{
		private static final long serialVersionUID = 1L;
		Boolean value = null;
		
		public int hashCode() {
	        return this.value.hashCode();
	    }
		public Bool(String value) {
			if(!value.equals("") && value != null)
				this.value = Boolean.valueOf(value);
		}
		public Bool(boolean bool) {
			this.value = bool;
		}
		public Boolean getValue() {
			return value;
		}
		public void setValue(Boolean value) {
			this.value = value;
		}
		public String toString() {
			return String.valueOf(value);
		}
		@Override
		public int compareTo(Datum o) 
		{
			return this.value.compareTo(((Bool)o).getValue());			
		}
		public int equals(Datum o) 
		{
			return this.value.compareTo(((Bool)o).getValue());			
		}
	}
	
	class Dt implements Datum, Serializable
	{
		private static final long serialVersionUID = 1L;
		Date value = null;
		
		public int hashCode() {
	        return this.value.hashCode();
	    }
		public Dt(String value) {
			if(!value.equals("") && value != null)
				this.value = Date.valueOf(value);
		}
		public Dt(Date value) {
			this.value = value;
		}
		public Dt(Dt v) {
			this.value = v.value;
		}
		public Dt(int year, int month, int date) {	
			//System.out.println("Datum.Dt const: "+ year+"-"+month+"-"+date);
			this.value = Date.valueOf(year+"-"+month+"-"+date);
			//System.out.println("Datum.Dt value: "+ value);
		}
		public Date getValue() {
			return value;
		}
		public void setValue(Date value) {
			this.value = value;
		}
		public String toString() {
			return String.valueOf(value);
		}
		@Override
		public int compareTo(Datum o) 
		{
			try
			{
				if(o instanceof Str)
				{
					Date d = Date.valueOf(((Str)o).getValue());
					return this.value.compareTo(d);
				}
				else // (o instanceof Dt)
				{
					return this.value.compareTo(((Dt) o).getValue());
				}				
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			return 0;
		}
		public int equals(Datum o) 
		{
			try
			{
				return this.value.compareTo(((Dt) o).getValue());					
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			return 0;
		}
		public int getYear() {
			String a = new SimpleDateFormat("yyyy").format(value);
			//System.out.println("Getyear: "+ value.toString() + " - " + a);
			return Integer.parseInt(a);			
		}
		public int getMonth() {
			String a = new SimpleDateFormat("MM").format(value);
			//System.out.println("Getmonth: " + value.toString() + " - " + a);
			return Integer.parseInt(a);
		}
		public int getDate() {
			//String a = new SimpleDateFormat("dd").format(value);
			//System.out.println("Getdare: "+ a);
			//System.out.println("Getdate: "+value.toString() + " - " + (new SimpleDateFormat("dd").format(value)));
			return Integer.parseInt(new SimpleDateFormat("dd").format(value));
		}		
	}

}