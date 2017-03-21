import java.lang.reflect.Executable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Function;

import javax.swing.text.TabableView;

public class main {
	public static int flag=1;
	public static int task_number;
	public static String addr="";
	
	// the main function to execute the whole program.
	// to run the program, there are 3 arguments: Thread number, task number, file address(task file).
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		task_number=Integer.valueOf(args[1]);
		addr=args[2];
		
    	 LinkedBlockingDeque<String> inlist =new LinkedBlockingDeque<String>();
    	 LinkedBlockingDeque<String> outlist =new LinkedBlockingDeque<String>();

		client client =new client();
		worker worker =new worker();
		Thread t1 =new Thread(client,"client");
		Thread w1 =new Thread(worker,"worker1");
		Thread w2 =new Thread(worker,"worker2");
		Thread w3 =new Thread(worker,"worker3");
		Thread w4 =new Thread(worker,"worker4");
		Thread w5 =new Thread(worker,"worker5");
		Thread w6 =new Thread(worker,"worker6");
		Thread w7 =new Thread(worker,"worker7");
		Thread w8 =new Thread(worker,"worker8");
		Thread w9 =new Thread(worker,"worker9");
		Thread w10 =new Thread(worker,"worker10");
		Thread w11 =new Thread(worker,"worker11");
		Thread w12 =new Thread(worker,"worker12");
		Thread w13 =new Thread(worker,"worker13");
		Thread w14 =new Thread(worker,"worker14");
		Thread w15 =new Thread(worker,"worker15");
		Thread w16 =new Thread(worker,"worker16");

		
		client.setList(inlist);
		worker.setList(inlist);
		client.setOutlist(outlist);
		worker.setOutlist(outlist);
		
		double start=System.currentTimeMillis();
		t1.start();
		if(Integer.valueOf(args[0])==1){
			w1.start();	
			
			t1.join();
			w1.join();
		}
		else if(Integer.valueOf(args[0])==2){
			w1.start();
			w2.start();
			
			t1.join();
			w1.join();
			w2.join();
		}
		else if(Integer.valueOf(args[0])==4){
			w1.start();
			w2.start();
			w3.start();
			w4.start();
			
			
			t1.join();
			w1.join();
			w2.join();
			w3.join();
			w4.join();
		}
		else if(Integer.valueOf(args[0])==8){
			w1.start();
			w2.start();
			w3.start();
			w4.start();
			w5.start();
			w6.start();
			w7.start();
			w8.start();
			
			t1.join();
			w1.join();
			w2.join();
			w3.join();
			w4.join();
			w5.join();
			w6.join();
			w7.join();
			w8.join();
		}
		else if(Integer.valueOf(args[0])==16){
			w1.start();
			w2.start();
			w3.start();
			w4.start();
			w5.start();
			w6.start();
			w7.start();
			w8.start();
			w9.start();
			w10.start();
			w11.start();
			w12.start();
			w13.start();
			w14.start();
			w15.start();
			w16.start();
			
			t1.join();
			w1.join();
			w2.join();
			w3.join();
			w4.join();
			w5.join();
			w6.join();
			w7.join();
			w8.join();
			w9.join();
			w10.join();
			w11.join();
			w12.join();
			w13.join();
			w14.join();
			w15.join();
			w16.join();
		}
		else{
			System.out.println("error");
			System.exit(-1);
		}
		double time=System.currentTimeMillis()-start;
		System.out.println("time: "+time+" ms");
		System.out.println("throughout: "+1000*(task_number/time)+" task/s");
		//t3.start();

	}

}
