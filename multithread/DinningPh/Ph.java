package multithread.task1;

import java.util.Random;

public class Ph extends Thread{
	public String name;
	public Fork left;
	public Fork right;
	public Ph (String name, Fork left, Fork right) {
		super(name);
		this.name = name;
		this.left = left;
		this.right = right;
	}
	
	public void eat() throws InterruptedException {
		Thread.sleep(new Random().nextInt()*10000);
	}
	
	public void think() throws InterruptedException{
		Thread.sleep(new Random().nextInt()*10000);
	}
	
	public static void main(String[] args) 
	{
		Fork fork1 = new Fork(1,true);
		Fork fork2 = new Fork(1,true);
		
		fork1.start();
		fork2.start();
		
		
	}
}