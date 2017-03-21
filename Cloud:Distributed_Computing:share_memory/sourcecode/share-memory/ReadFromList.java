import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Queue;

//read each part of data which was stored in list, then quicksort and output to file.
public class ReadFromList implements Runnable  {
	
	 private Queue<String[][]> list;
	 private Queue<String[][]> outlist;
	 private Object parent; 

	 public Object getParent() {
		return parent;
	}

	public void setParent(Object parent) {
		this.parent = parent;
	}

	public Queue<String[][]> getList() {
		return list;
	}

	public void setList(Queue<String[][]> list) {
		this.list = list;
	}

	public Queue<String[][]> getOutlist() {
		return outlist;
	}

	public void setOutlist(Queue<String[][]> outlist) {
		this.outlist = outlist;
	}



   
    public void run(){
 
        int filenumber = 0;
        while(true){
			try {
				
				
				outlist.clear();
				String[][] x=new String[TeraSort.divide_block][2];
		        synchronized (list){

		            while(list.size()==0&TeraSort.end_flag!=1){
		            	list.wait();
		            }
		            if(list.size()==0&TeraSort.end_flag==1){
		            	//list.notifyAll();
		            	break;
		            }
		            synchronized (parent) {
						TeraSort.file_number++;
						filenumber = TeraSort.file_number;
					}
	            	x=list.poll();            	
	            	QuickSort(x, 1, x.length);
	            	outlist.add(x);
	            	list.notifyAll();
	            	
	                FileWriter out=null;
	            	out = new FileWriter(new File(TeraSort.file_addr+"/outlist/outlist"+filenumber+".txt"));
					for(String[][] output: outlist){
						for(int i=0;i<output.length;i++){
								out.write(output[i][1]+"\n");							
						}
					}

			
					out.close();
					if(list.size()==0){
		            	TeraSort.end_flag=1;
		            	break;
		            }
		        }
			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}

    	}

	}
    

 
     

   	
   	
   	

    

    public static void print(String[][] data){
    	for(int a=0;a<data.length;a++){
    		for(int b=0;b<data[0].length;b++){
    			System.out.print(data[a][b]+",");
    		}
    		System.out.println();
    	}
    }
    
	  
	 public static int Partition(String[][] a,int p,int r){
		int i=p-1;
		String temp0,temp1;
	    for(int j=p;j<=r-1;j++){
	    	if(a[j-1][0].compareTo(a[r-1][0])<=0){
	    		// swap(a[j-1],a[i-1]);
	    		i++;
	    		temp0=a[j-1][0];
	    		temp1=a[j-1][1];
	    		a[j-1][0]=a[i-1][0];
	    		a[j-1][1]=a[i-1][1];
	    		a[i-1][0]=temp0;
	    		a[i-1][1]=temp1;

}
	    }
	    //swap(a[r-1,a[i+1-1]);
	    temp0=a[r-1][0];
	    temp1=a[r-1][1];
	    a[r-1][0]=a[i+1-1][0];
	    a[r-1][1]=a[i+1-1][1];
	    a[i+1-1][0]=temp0;
	    a[i+1-1][1]=temp1;
	    
	    return i+1;

}
	 public static void QuickSort(String a[][],int p,int r){
		
		if(p<r){
			int q=Partition(a,p,r);
			QuickSort(a,p,q-1);
			QuickSort(a,q+1,r);
			
		}
		
	}













}



