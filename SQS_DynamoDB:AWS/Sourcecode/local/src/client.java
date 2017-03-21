import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

import javax.print.attribute.standard.PrinterLocation;

public class client implements Runnable  {
	
	//the first queue to store the tasks
	private LinkedBlockingDeque<String> list =new LinkedBlockingDeque<String>();
	public LinkedBlockingDeque<String> getList() {
		return list;
	}

	public void setList(LinkedBlockingDeque<String> list) {
		this.list = list;
	}





	// the second queue to store the successful execution messages
	private LinkedBlockingDeque<String> outlist =new LinkedBlockingDeque<String>();

	public LinkedBlockingDeque<String> getOutlist() {
		return outlist;
	}

	public void setOutlist(LinkedBlockingDeque<String> outlist) {
		this.outlist = outlist;
	}






	@SuppressWarnings("resource")
	public void client_queue() throws InterruptedException, IOException{

		int i=0;

	  	 String line="";
		 BufferedReader file = null;
		 
		 //read tasks from txt file,
  		while(true){
				if(i==main.task_number){
  					break;
  				}
  			synchronized (list){

  				try {
  					file = new BufferedReader(new InputStreamReader(new FileInputStream(main.addr)));
  				} catch (FileNotFoundException e1) {
  					e1.printStackTrace();
  				}
  				line=file.readLine();

  				list.add(i+line);
  				i++;
  				list.notifyAll();
  				file.close();
  		}
	  		}
  		

  	  	  main.flag=0;


  		
  		
  		// when all tasks are executed by workers, show the number of tasks executed successfully
	  	while(true){
	  		System.out.print("");
	  		if(worker.flag_check==1){
	  			System.out.println(main.task_number+" succeed");
	  			break;
	  		}
	  		
	  	}
	  	for(String a:outlist){
	  		System.out.println(a);
	  		
	  	}
	  	System.out.println(outlist.size()+" tasks succeed");
  		
	
	}
		
	  	
	
		
	
   
    public void run(){
		try {
			client_queue();
		} catch (InterruptedException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    


		

		
}






