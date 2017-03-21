import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

import org.w3c.dom.ls.LSInput;

public class worker implements Runnable  {
	


	public static int flag_check=0;
	public static int loop=0;

	private LinkedBlockingDeque<String> list =new LinkedBlockingDeque<String>();
	public LinkedBlockingDeque<String> getList() {
		return list;
	}

	public void setList(LinkedBlockingDeque<String> list) {
		this.list = list;
	}

	private LinkedBlockingDeque<String> outlist =new LinkedBlockingDeque<String>();

	
	public LinkedBlockingDeque<String> getOutlist() {
		return outlist;
	}

	public void setOutlist(LinkedBlockingDeque<String> outlist) {
		this.outlist = outlist;
	}






	public void worker_queue() throws InterruptedException{
		String task="task";
		// read the command from the first queue, than execute the sleep workload, if succeed, store the succeed message in the second queue.
		while(true){
	  				if(list.size()==0&main.flag==0){
	  					flag_check=1;
	  					break;
	  				}
	  				if(list.size()>0){

	  				task=list.poll();
	  				
		  				String[] a=task.split(" ");
		  				if(a[1].equals("sleep")){
		  					Thread.sleep(Integer.valueOf(a[2]));
		  					loop++;
		  					outlist.add(Thread.currentThread().getName()+" task "+a[0]+" succeed");
		  				}
	  				
		  				System.out.println(task);
	  				


	  		}
		}
	
	}
		
	  	
	
		
	
   
    public void run(){
		try {
			worker_queue();
		} catch (InterruptedException e) {
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






