package ProCon;

public class Consumer extends Thread{
	private int conRate;
	private Storage store;
	
	public Consumer(int rate, Storage store) {
		super();
		this.conRate = rate;
		this.store = store;
	}
	
	public int getRate() {
		return this.conRate;
	}
	
	public int getSize() {
		return this.store.getSize();
	}
	
	public void run() {
		try {
			this.store.consume(conRate);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}