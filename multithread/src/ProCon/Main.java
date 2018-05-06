package ProCon;

public class Main{
	public static void main(String[] args) throws InterruptedException {
		Storage store = new Storage(10);
		Producer pro1 = new Producer(2, store);
		Producer pro2 = new Producer(4, store);
		Producer pro3 = new Producer(1, store);
		Producer pro4 = new Producer(2, store);
		
		Consumer con1 = new Consumer(3, store);
		Consumer con2 = new Consumer(2, store);
		Consumer con3 = new Consumer(1, store);
		
		
		pro1.start();
		pro2.start();
		pro3.start();
		pro4.start();
		
		con1.start();
		con2.start();
		con3.start();
		
		pro1.join();
		pro2.join();
		pro3.join();
		pro4.join();
		con1.join();
		con2.join();
		con3.join();
	}
}