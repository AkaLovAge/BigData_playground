package ProCon;

import java.util.LinkedList;

public class Storage{
	private int MAX_CAP;
	private LinkedList<Integer> store = new LinkedList<>();
	
	public Storage(int cap) {
		super();
		this.MAX_CAP = cap;
	}
	public LinkedList getList() {
		return this.store;
	}
	
	public int getCap() {
		return this.MAX_CAP;
	}
	
	public int getSize() {
		return this.store.size();
	}
	public void produce(int num) throws Exception{
		synchronized(store) {
			while (num + store.size() > MAX_CAP) {
				System.out.println("producer " + Thread.currentThread().getName()+" has no enough space, wait");
				store.wait();
			} 
			System.out.println("producer " + Thread.currentThread().getName()+" going to produce " + num + " products");
			for (int i=0; i < num; i++) {
				this.store.add(1);
			
			}
			store.notify();
		}
	}
	
	public void consume(int num) throws Exception{
		synchronized(store) {
			while (store.size() < num) {
				System.out.println("consumer " + Thread.currentThread().getName()+" has no enough product, wait");
				store.wait();
			}
			System.out.println("consumer " + Thread.currentThread().getName()+" going to consume " + num + " products");
			for (int i=0; i< num; i++) {
				this.store.pop();
			}
			store.notify();
		}
	}
}