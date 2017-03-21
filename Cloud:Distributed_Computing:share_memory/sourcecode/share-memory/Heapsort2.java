import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Queue;

 
//read data from each datafile, then heapsort and output to 1 file

public class Heapsort2 implements Runnable {

		static int file_number=TeraSort.file_number;
		@SuppressWarnings("rawtypes")
		private static Queue[] queueArr = new Queue[file_number+1];
		private static int[] flag=new int[file_number+1];

		public static int[] getFlag() {
			return flag;
		}

		public static void setFlag(int[] flag) {
			Heapsort2.flag = flag;
		}

		@SuppressWarnings("rawtypes")
		public static Queue[] getQueueArr() {
			return queueArr;
		}

		@SuppressWarnings("rawtypes")
		public static void setQueueArr(Queue[] queueArr) {
			Heapsort2.queueArr = queueArr;
		}

		@Override
		public void run() {
			
	    	PriorityQueue<Node> heap =new PriorityQueue<>();
			FileWriter out=null;
			File file =new File(TeraSort.file_addr+"/final_outlist.txt");
			if(file.exists()){
				file.delete();
			}
			try {
				out = new FileWriter(file,true);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
			for(int j=1;j<=file_number;j++){
				synchronized (queueArr[j]){
					while(queueArr[j].size()==0&flag[j]!=0){
						try {
							queueArr[j].wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					
					if(queueArr[j].isEmpty()==false){
						heap.add((Node)queueArr[j].poll());	   
						}
				}
			}

//			 int l=1;
			 while(true){
	    		if(heap.isEmpty()){
	    			break;
	    		}
	        	Node poll=heap.poll();
	        	
	        	try {
					out.write(poll.getKey()+"\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
	        	
//	    		System.out.println(l);
//	    		l++;
	    		synchronized (queueArr[poll.getIndex()]) {
		    		while(queueArr[poll.getIndex()].size()==0&flag[poll.getIndex()]!=0){	    			
		    			try {
							queueArr[poll.getIndex()].wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}		    			
		    		}
		    		if(queueArr[poll.getIndex()].size()==0){
		    			continue;
		    		}
		          	heap.add((Node)queueArr[poll.getIndex()].poll());
		    		queueArr[poll.getIndex()].notify();

				}


			    	}
			    	try {
						out.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
		}
}
