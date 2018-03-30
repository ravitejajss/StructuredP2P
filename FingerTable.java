import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
/**
 * Short one line description.                           (1)
 * <p>
 * Longer description. If there were any, it would be    (2)
 * here.
 * <p>
 * And even more explanations to follow in consecutive
 * paragraphs separated by HTML paragraph breaks.
 *
 * @param  variable Description text text text.          (3)
 * @return Description text text text.
 */
public class FingerTable {
	private int myKey;
	private int myPort;
	private String myIp;
	private int m;
	private int keyLimit;
	//private int totalNodeCount;
	private ConcurrentHashMap<Integer, Integer> fingerMap = new ConcurrentHashMap<Integer, Integer>();
	private ConcurrentHashMap<Integer, ArrayList<String>> allNodeDetails = new ConcurrentHashMap<Integer, ArrayList<String>>();
	private List<Integer> startKeys = Collections.synchronizedList(new ArrayList<Integer>());
	private List<Integer> allKeysInOrder = Collections.synchronizedList(new ArrayList<Integer>());
	
	FingerTable(int nodeKey, String nodeIp, int nodePort, int m, int totalNodeCount){
		this.myKey = nodeKey;
		this.myIp = nodeIp;
		this.myPort = nodePort;
		this.m = m;
		//this.totalNodeCount = totalNodeCount;
		this.keyLimit = (int) Math.pow(2, m);
		FingerTableInit();
	}
	
	private void FingerTableInit() {
		for (int i = 1; i <= m; i++) {
			int start = (int) ( (Math.pow(2, i-1) + this.myKey) % Math.pow(2, m));
			startKeys.add(start);
		}
		
		ArrayList<String> details = new ArrayList<String>();
		details.add(0,myIp);
		details.add(1,Integer.toString(myPort));
		allNodeDetails.put(this.myKey, details);
		allKeysInOrder.add(this.myKey);
		
		for (int i : startKeys) {
			fingerMap.put(i, this.myKey);
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

	public int FindPrevKey(int nodeKey) {
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
	
	public int FindNextKey(int nodeKey) {
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
			System.out.println(keysInOrder+"|"+nodeKey);
			if(keysInOrder.indexOf(nodeKey)<keysInOrder.size()-1) {
				return fingerMap.get(keysInOrder.get(keysInOrder.indexOf(nodeKey) + 1));
			}
			else {
				return fingerMap.get(keysInOrder.get(0));
			}
		}
	}
	
	/**
	 * Short one line description.                           (1)
	 * <p>
	 * Longer description. If there were any, it would be    (2)
	 * here.
	 * <p>
	 * And even more explanations to follow in consecutive
	 * paragraphs separated by HTML paragraph breaks.
	 *
	 * @param  variable Description text text text.          (3)
	 * @return Description text text text.
	 */
	public void AddEntry(int nodeKey, String ip, int port) {
		if (nodeKey>keyLimit){
			System.out.println("node key out of range");
			return;
		}
		AddDetails(nodeKey, ip, port);
		Update();
	}
	
	/**
	 * Short one line description.                           (1)
	 * <p>
	 * Longer description. If there were any, it would be    (2)
	 * here.
	 * <p>
	 * And even more explanations to follow in consecutive
	 * paragraphs separated by HTML paragraph breaks.
	 *
	 * @param  variable Description text text text.          (3)
	 * @return Description text text text.
	 */
	public void AddDetails(int nodeKey, String ip, int port) {
		if (nodeKey>keyLimit){
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
	/**
	 * 
	 * @param key
	 * @return
	 */
	public String GetIp(int key) {
		return allNodeDetails.get(fingerMap.get(FindPrevKey(key))).get(0);
	}
	
	public int GetPort(int key) {
		return Integer.parseInt(allNodeDetails.get(fingerMap.get(FindPrevKey(key))).get(1));
	}

	public int GetSuccessor(int key) {
		ArrayList<Integer> keysInOrder = new ArrayList<Integer>();
		for (int i : fingerMap.values()) {
			keysInOrder.add(i);
		}
		keysInOrder.add(key);
		Collections.sort(keysInOrder);
		int myIndex = keysInOrder.indexOf(key);
		if(myIndex==keysInOrder.size()-1) {
			return keysInOrder.get(0);
		}
		else {
			return  keysInOrder.get(myIndex+1);
		}
	}
	
	public int GetPredecessor(int key) {
		ArrayList<Integer> keysInOrder = new ArrayList<Integer>();
		for (int i : fingerMap.values()) {
			keysInOrder.add(i);
		}
		keysInOrder.add(key);
		Collections.sort(keysInOrder);
		int myIndex = keysInOrder.indexOf(key);
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
				String.format("%05d", startKeys.get(startKeys.size()-1)) +","+ String.format("%05d", this.myKey)
				+")      " +String.format("%05d", fingerMap.get(startKeys.get(startKeys.size()-1))));
	}
	
	public void RemoveEntry(int key) {
		System.out.println("removing entry from allkeysinorder");
		allKeysInOrder.remove(new Integer(key));
		System.out.println("removing entry from allNodeDetails");
		allNodeDetails.remove(key);
		System.out.println("updating fingertable");
		Update();
	}
	
	public void PrintStats() {
		System.out.println(allKeysInOrder+"\n");
		for(int i : allNodeDetails.keySet())
			System.out.println(i+": "+allNodeDetails.get(i));
		System.out.println("\n");
		Print();
		System.out.println("======");
	}
}
