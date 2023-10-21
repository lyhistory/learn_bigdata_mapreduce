package com.lyhistory.parallelism;

import java.util.HashMap;
import java.util.List;

public class MyReducer implements Reducer{

	@Override
	public HashMap reduce(Object key, List data) {
		// TODO Auto-generated method stub
		System.out.println(Thread.currentThread().getName()+" START reduce");
		HashMap result=new HashMap();
		long total=0;
		/*if(Thread.currentThread().getName().equals("Thread-5")){
			System.out.println("---------------------------------------");
			System.out.println(Thread.currentThread().getName()+"START");
			System.out.print("key="+key+"::");
		}*/
		for(int i=0,count=data.size();i<count;++i){
			long sum=0;
			/*if(Thread.currentThread().getName().equals("Thread-5")){
				System.out.print(data.get(i)+",");
			}*/
			String[] list=data.get(i).toString().
				replaceAll(" ", "").
				replaceAll("\\[", "").
				replaceAll("\\]", "").
				split(",");
			for(int j=0,innercount=list.length;j<innercount;++j){
				//System.out.println(list[j]+"|");
				sum+=Long.parseLong(list[j].toString());
			}
			total+=sum;
			/*if(Thread.currentThread().getName().equals("Thread-5")){
				System.out.print("~~SUM="+sum);
			}*/
		}
		/*if(Thread.currentThread().getName().equals("Thread-5")){
			System.out.println(Thread.currentThread().getName()+"END");
			System.out.println("---------------------------------------");
		}*/
		result.put(key, total);
		System.out.println(Thread.currentThread().getName()+" END reduce");
		return result;
	}

}
