import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.Queue;


//get data from 1G or 10G file
public class writeTolist implements Runnable  {
	



	private Queue<String[][]> list = new LinkedList<String[][]>();
    public Queue<String[][]> getList() {
		return list;
	}

	public void setList(Queue<String[][]> list){
		this.list = list;
	}
    public void datatolist() throws InterruptedException, IOException{    
    	
    	String filename=TeraSort.file_addr+"/data.txt";
	    String[][] allocation=new String[TeraSort.divide_block][2];
	  	BufferedReader file = null;
		try {
			file = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
	  	 String line="";
	
	  	synchronized (list){
	  	 while(TeraSort.i<TeraSort.total_line){
     		   while(list.size()==8){   			   
       			   list.wait();
       		   }
     		   
	  	   	try {
				line = file.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
	  	   	if(line==null){
	  	   		TeraSort.end_flag=1;
	  	   		list.notifyAll();
	  	   		break;
	  	   	}
	          allocation[TeraSort.j][0]=line.substring(0,10);
	          allocation[TeraSort.j][1]=line.substring(0,98);
	          TeraSort.i++;TeraSort.j++;
	          if(TeraSort.i==TeraSort.k1){
	        	  list.add(allocation);
		       	  TeraSort.j=0;
		       	  TeraSort.k1=TeraSort.k1+TeraSort.k2;
		          allocation=new String[TeraSort.divide_block][2];
	          }

	 	  	 list.notifyAll();
		  	}

	  	}		 
		file.close();	    	
	  	}	
   
    public void run(){
		try {
			datatolist();
			
		} catch (InterruptedException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
    
	 


		

		
}






