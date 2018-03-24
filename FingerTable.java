import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class FingerTable {
	private int nodeKey;
	private int nodePort;
	private String nodeIp;
	private int totalNodeCount;
	private ConcurrentHashMap<Integer, Integer> fingerMap = new ConcurrentHashMap<Integer, Integer>();
	private ConcurrentHashMap<Integer, ArrayList<String>> allNodeDetails = new ConcurrentHashMap<Integer, ArrayList<String>>();
	private ArrayList<Integer> startKeys = new ArrayList<Integer>();
	private ArrayList<Integer> allKeysInOrder = new ArrayList<Integer>();
	
	FingerTable(int node, String nodeIp, int nodePort, int totalNodeCount){
		this.nodeKey = node;
		this.nodeIp = nodeIp;
		this.nodePort = nodePort;
		this.totalNodeCount = totalNodeCount;
		FingerTableInit();
	}
	
	private void FingerTableInit() {
		int m = (int) Math.ceil(Math.log(totalNodeCount) / Math.log(2));
		for (int i = 1; i <= m; i++) {
			int start = (int) ( (Math.pow(2, i-1) + nodeKey) % totalNodeCount);
			startKeys.add(start);
		}
		
		ArrayList<String> details = new ArrayList<String>();
		details.add(0,nodeIp);
		details.add(1,Integer.toString(nodePort));
		allNodeDetails.put(nodeKey, details);
		allKeysInOrder.add(nodeKey);
		
		for (int i : startKeys) {
			fingerMap.put(i, nodeKey);
		}
	}
	
	private int FindKey(int nodeKey) {
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
		ArrayList<String> details = new ArrayList<String>();
		details.add(0,ip);
		details.add(1,Integer.toString(port));
		allNodeDetails.put(nodeKey, details);
		if(!allKeysInOrder.contains(nodeKey)) {
			allKeysInOrder.add(nodeKey);	
		}
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
	
	public String GetIp(int key) {
		return allNodeDetails.get(fingerMap.get(FindKey(key))).get(0);
	}

	public int GetPort(int key) {
		return Integer.parseInt(allNodeDetails.get(fingerMap.get(FindKey(key))).get(1));
	}
	
	public void Print() {
		System.out.println("Start   Interval     Successor");
		for ( int i = 0; i < startKeys.size()-1; i++) {
			System.out.println(String.format("%02d", startKeys.get(i))+ "      ["+String.format("%02d", startKeys.get(i)) +","+ String.format("%02d", startKeys.get(i+1)) +")      " +String.format("%02d", fingerMap.get(startKeys.get(i))));
		}
		System.out.println(String.format("%02d", startKeys.get(startKeys.size()-1))+ "      ["+ String.format("%02d", startKeys.get(startKeys.size()-1)) +","+ String.format("%02d", this.nodeKey) +")      " +String.format("%02d", fingerMap.get(startKeys.get(startKeys.size()-1))));
	}
	
	public void RemoveEntry(int key) {
		//Write this part
		//handle entry not there
		
	}
	
	public void PrintStats() {
		System.out.println(allKeysInOrder);
	}
}
