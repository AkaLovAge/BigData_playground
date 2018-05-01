package DinningPH;

public class Fork extends Thread{
	int id;
	boolean status;
	public Fork(int id, boolean status) {
		this.id = id;
		this.status = status;
	}

	public synchronized void take() throws InterruptedException {
		if (this.status) {
			System.out.println(Thread.currentThread().getName() + " take fork" + this.id);
			this.status = false;
		}else {
			wait();
		}
	}
	
	public synchronized void put() throws InterruptedException {
		if (!this.status) {
			System.out.println(Thread.currentThread().getName() + " put fork" + this.id);
			this.status = true;
			notify();
		}
	}
	
	public void run() {
		for (int i=0; i< 200; i ++) {
			try {
				take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			try {
				put();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}