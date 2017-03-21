import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

//main function to excute the sort program, can run different number of threads
public class TeraSort{
	public static int end_flag=0,queueArr_size,Thread_number;
	public static int i=0,j=0,divide_block,k1,k2,total_line=0,file_number=0;//10731520 16384
	public static String file_addr;
    @SuppressWarnings({ "deprecation", "static-access", "rawtypes" })
	public static void main(String []args) throws IOException, InterruptedException{

    	TeraSort teraSort = new TeraSort();
    	teraSort.run(args);
   	
    }
    
	
    
    public void run(String[] args) throws InterruptedException {

    	TeraSort.total_line = Integer.parseInt(args[0]);
    	TeraSort.divide_block = Integer.parseInt(args[1]);
    	TeraSort.file_addr=args[2];
    	TeraSort.queueArr_size=Integer.parseInt(args[3]);
    	TeraSort.Thread_number=Integer.parseInt(args[4]);
    	k1=TeraSort.divide_block;
    	k2=TeraSort.divide_block;
//    	
    	Queue<String[][]> inlist = new LinkedList<String[][]>();
      	Queue<String[][]> outlist = new LinkedList<String[][]>();
//  	  	
  	  	  	  	
      	writeTolist t1 = new writeTolist();
      	ReadFromList t2 = new ReadFromList();
      	
//		
		t1.setList(inlist);
		t2.setList(inlist);
		t2.setOutlist(outlist);
		t2.setParent(this);
		Thread thread1 =new Thread(t1,"thread1");
		
		Thread thread2 = new Thread(t2,"thread2");
		Thread thread21 = new Thread(t2,"thread21");
		Thread thread22 = new Thread(t2,"thread22");
		Thread thread23 = new Thread(t2,"thread23");
		System.out.println("Start to input data from file and divide sort...");
		long start,end,TOTAL_START,TOTAL_END;
		start=System.currentTimeMillis();
		TOTAL_START=System.currentTimeMillis();
		thread1.start();

		if(TeraSort.Thread_number==1){//1 threads
			thread2.start();
			
			
			thread1.join();
			thread2.join();
			
		}
		else if(TeraSort.Thread_number==2){//2 threads
			thread2.start();
			thread21.start();
			
			
			thread1.join();
			thread2.join();
			thread21.join();
		}else if(TeraSort.Thread_number==3){// 3 threads
			thread2.start();
			thread21.start();
			thread22.start();
			
			thread1.join();
			thread2.join();
			thread21.join();
			thread22.join();
		}else if(TeraSort.Thread_number==4){//4 threads
			thread2.start();
			thread21.start();
			thread22.start();
			thread23.start();
			
			thread1.join();
			thread2.join();
			thread21.join();
			thread22.join();
			thread23.join();
		}

		end =System.currentTimeMillis();
		System.out.println("Divide time:"+(end-start)+" ms");

		
		Queue[] inqueueArr = new Queue[file_number+1];
		int[] flag=new int[file_number+1];
		Heapsort h1 =new Heapsort(); 
		Heapsort2 h2 =new Heapsort2();
		
		h1.setQueueArr(inqueueArr);
		h2.setQueueArr(inqueueArr);
		h1.setFlag(flag);
		h2.setFlag(flag);

		Thread thread3 =new Thread(h1,"h1");
		Thread thread4 =new Thread(h2,"h2");
		
		System.out.println("Start to Heapsort...");
		start=System.currentTimeMillis();
		thread3.start();
		thread4.start();
		thread4.join();
		thread3.stop();
		end =System.currentTimeMillis();
		TOTAL_END=System.currentTimeMillis();
		
		System.out.println("Heapsort time:"+(end-start)+" ms");
		System.out.println("Total time:"+(TOTAL_END-TOTAL_START)+" ms");

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