import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class FingerTable {
	private int nodeKey;
	private int nodePort;
	private String nodeIp;
	private int m;
	private int totalNodeCount;
	private ConcurrentHashMap<Integer, Integer> fingerMap = new ConcurrentHashMap<Integer, Integer>();
	private ConcurrentHashMap<Integer, ArrayList<String>> allNodeDetails = new ConcurrentHashMap<Integer, ArrayList<String>>();
	private List<Integer> startKeys = Collections.synchronizedList(new ArrayList<Integer>());
	private List<Integer> allKeysInOrder = Collections.synchronizedList(new ArrayList<Integer>());
	
	FingerTable(int node, String nodeIp, int nodePort, int m, int totalNodeCount){
		this.nodeKey = node;
		this.nodeIp = nodeIp;
		this.nodePort = nodePort;
		this.m = m;
		this.totalNodeCount = totalNodeCount;
		FingerTableInit();
	}
	
	private void FingerTableInit() {
		for (int i = 1; i <= m; i++) {
			int start = (int) ( (Math.pow(2, i-1) + this.nodeKey) % Math.pow(2, m));
			startKeys.add(start);
		}
		
		ArrayList<String> details = new ArrayList<String>();
		details.add(0,nodeIp);
		details.add(1,Integer.toString(nodePort));
		allNodeDetails.put(this.nodeKey, details);
		allKeysInOrder.add(this.nodeKey);
		
		for (int i : startKeys) {
			fingerMap.put(i, this.nodeKey);
		}
	}
	
	private void Update() {
		Collections.sort(allKeysInOrder);
		
		for (int i : startKeys) {
			if (allNodeDetails.keySet().contains(i)) {
				fingerMap.put(i, i);
			}
			else {
				ArrayList<Integer> keysInOrder = new ArrayList<Integer>();
				for (int key : allKeysInOrder) {
					keysInOrder.add(key);
				}
				keysInOrder.add(i);
				Collections.sort(keysInOrder);
				if (keysInOrder.indexOf(i)+1==keysInOrder.size()){
					fingerMap.put(i, keysInOrder.get(0));
				}
				else {
					fingerMap.put(i, keysInOrder.get(keysInOrder.indexOf(i)+1));
				}
			}
		}
	}

	public int FindKey(int nodeKey) {
		if (startKeys.contains(nodeKey)) {
			return nodeKey;
		}
		else {
			ArrayList<Integer> keysInOrder = new ArrayList<Integer>();
			for (int i : startKeys) {
				keysInOrder.add(i);
			}
			keysInOrder.add(nodeKey);
			Collections.sort(keysInOrder);
			if(keysInOrder.indexOf(nodeKey)!=0) {
				return keysInOrder.get(keysInOrder.indexOf(nodeKey) - 1);
			}
			else {
				return keysInOrder.get(keysInOrder.size() - 1);
			}
		}
	}
	
	public void AddEntry(int nodeKey, String ip, int port) {
		if (nodeKey>totalNodeCount){
			System.out.println("node key out of range");
			return;
		}
		AddDetails(nodeKey, ip, port);
		Update();
	}
	
	public void AddDetails(int nodeKey, String ip, int port) {
		if (nodeKey>totalNodeCount){
			System.out.println("node key out of range");
			return;
		}
		ArrayList<String> details = new ArrayList<String>();
		details.add(0,ip);
		details.add(1,Integer.toString(port));
		allNodeDetails.put(nodeKey, details);
		if(!allKeysInOrder.contains(nodeKey)) {
			allKeysInOrder.add(nodeKey);	
		}
		Collections.sort(allKeysInOrder);
	}
	
	public String GetIp(int key) {
		return allNodeDetails.get(fingerMap.get(FindKey(key))).get(0);
	}
	
	public int GetPort(int key) {
		return Integer.parseInt(allNodeDetails.get(fingerMap.get(FindKey(key))).get(1));
	}

	public int GetSuccessor() {
		ArrayList<Integer> keysInOrder = new ArrayList<Integer>();
		for (int i : startKeys) {
			keysInOrder.add(i);
		}
		keysInOrder.add(nodeKey);
		Collections.sort(keysInOrder);
		int myIndex = keysInOrder.indexOf(nodeKey);
		if(myIndex==keysInOrder.size()-1) {
			return keysInOrder.get(0);
		}
		else {
			return keysInOrder.get(myIndex+1);
		}
	}
	
	public int GetPredecessor() {
		ArrayList<Integer> keysInOrder = new ArrayList<Integer>();
		for (int i : startKeys) {
			keysInOrder.add(i);
		}
		keysInOrder.add(nodeKey);
		Collections.sort(keysInOrder);
		int myIndex = keysInOrder.indexOf(nodeKey);
		if(myIndex==0) {
			return keysInOrder.get(keysInOrder.size()-1);
		}
		else {
			return keysInOrder.get(myIndex-1);
		}
	}

	
	public void Print() {
		System.out.println("Start      Interval           Successor");
		for ( int i = 0; i < startKeys.size()-1; i++) {
			System.out.println(String.format("%05d", startKeys.get(i))+ "      ["
					+String.format("%05d", startKeys.get(i)) +","+ String.format("%05d", startKeys.get(i+1)) +")      "
					+String.format("%05d", fingerMap.get(startKeys.get(i))));
		}
		System.out.println(String.format("%05d", startKeys.get(startKeys.size()-1))+ "      ["+
				String.format("%05d", startKeys.get(startKeys.size()-1)) +","+ String.format("%05d", this.nodeKey)
				+")      " +String.format("%05d", fingerMap.get(startKeys.get(startKeys.size()-1))));
	}
	
	public void RemoveEntry(int key) {
		allKeysInOrder.remove(new Integer(key));
		allNodeDetails.remove(key);
		Update();
	}
	
	public void PrintStats() {
		System.out.println(allKeysInOrder);
	}
}
