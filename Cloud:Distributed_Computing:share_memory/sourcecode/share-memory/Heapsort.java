import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.Queue;

//read data from each datafile, then heapsort and output to 1 file

public class Heapsort implements Runnable {
	@SuppressWarnings("rawtypes")
	static int file_number=TeraSort.file_number;
	static int queueArr_size=TeraSort.queueArr_size;
	private static Queue[] queueArr = new Queue[file_number+1];
	private static int[] flag=new int[file_number+1];

	public static int[] getFlag() {
		return flag;
	}




	public static void setFlag(int[] flag) {
		Heapsort.flag = flag;
	}




	@SuppressWarnings("rawtypes")
	public static Queue[] getQueueArr() {
		return queueArr;
	}




	@SuppressWarnings("rawtypes")
	public static void setQueueArr(Queue[] queueArr) {
		Heapsort.queueArr = queueArr;
	}




	public void run() {
		try {
			read();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}		
	

	

	@SuppressWarnings({ "unchecked" })
	public  void read() throws IOException, InterruptedException{
		
		for(int i=0;i<flag.length;i++){
			flag[i]=i;
		}
		for (int i=0; i <queueArr.length; i++) {
			queueArr[i] = new LinkedList<String>();
		}

    	
		

    	BufferedReader[] file =new BufferedReader[file_number+1];
    	String line="";
    	for(int i=1;i<=file_number;i++){
    		String filename=TeraSort.file_addr+"/outlist/outlist"+i+".txt";
    		file[i] = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
    	} 
        
    	while(true){

    		for(int i=1;i<=file_number;i++){
    			synchronized(queueArr[i]){
    				while(true){
	        	  		if(queueArr[i].size()==queueArr_size)
	        	  		{   
	        	  			queueArr[i].notify();
	        	  			break;
	        	  		}
	        	  			
	        	  		line=file[i].readLine();
	        	  		if(line==null){
	        	  				queueArr[i].notify();
	        	  				flag[i]=0;
	        	  				break;
	        	  		}
	        	  		Node obj=new Node(line, i);
	        	  		queueArr[i].add(obj);
        	  		}
    			}
    		}

		}
    	
	}
    	



		



	
	public static void print2(String[] data){
    	for(int a=0;a<data.length;a++){
    			System.out.println(data[a]);
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



	











