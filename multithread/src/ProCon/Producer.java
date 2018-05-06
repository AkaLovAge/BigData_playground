package ProCon;

public class Producer extends Thread{
	private int proRate;
	private Storage store;
	
	public Producer(int rate, Storage store) {
		super();
		this.proRate = rate;
		this.store = store;
	}
	
	public int getRate() {
		return this.proRate;
	}
	
	public int getSize() {
		return this.store.getSize();
	}
	
	public void run() {
		try {
			this.store.produce(proRate);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}