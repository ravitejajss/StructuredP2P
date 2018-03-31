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
	private List<Integer> startKeys = Collections.synchronizedList(new ArrayList<Integer>());
	private List<Integer> allKeysInOrder = Collections.synchronizedList(new ArrayList<Integer>());
	private ConcurrentHashMap<Integer, ArrayList<String>> allNodeDetails = new ConcurrentHashMap<Integer, ArrayList<String>>();
	
	/**
	 * <tt>FingerTable(int nodeKey, String nodeIp, int nodePort,  int m,  int totalNodeCount)</tt>
	 * <p>
	 * FingerTable constructor
	 * <p>
	 * This method initializes all the variable in the class and runs FingerTableInit() method.
	 * This method is responsible the filling of the Finger Table for the first time.
	 *
	 * @param  nodeKey the value of the hash key of the node that calls this Finger Table.
	 * @param  nodeIp the IP string of the node that calls this Finger Table.
	 * @param  nodePort the value of the port of the node that calls this Finger Table.
	 * @param  m the value of the number of bits of the hash key that is being used.
	 * @param  totalNodeCount the value of the total number of nodes that this network contains (currently not in use).
	 * @return An initialized FingerTable object :)
	 */
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
	
	/**
	 * <tt>private void FingerTable.Update()</tt>
	 * <p>
	 * This method is responsible the filling of the Finger Table for the first time.
	 * <p>
	 * This method updates the finger table. It does so using the elements in the allKeysInOrder and NodeDetails
	 * Update them using {@link FingerTable.AddDetails()} before calling this method for correct functionality.
	 */
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

	/**
	 * <tt>public int FindPrevKey(int nodeKey)</tt>
	 * <p>
	 * Finds the previous key in the start keys to the given nodeKey and returns its mapping in finger table.
	 * 
	 * @param  nodeKey the value of the hash key of the node whose previous key in the Finger Table is needed.
	 * @return value of the previous node key
	 */
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
				return fingerMap.get(keysInOrder.get(keysInOrder.indexOf(nodeKey) - 1));
			}
			else {
				return fingerMap.get(keysInOrder.get(keysInOrder.size() - 1));
			}
		}
	}
	
	/**
	 * <tt>	public int FindNextKey(int nodeKey)</tt>
	 * <p>
	 * Finds the next key in the start keys to the given nodeKey and returns its mapping in finger table.
	 * 
	 * @param  nodeKey the value of the hash key of the node whose next key in the Finger Table is needed.
	 * @return value of the next node key
	 */
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
	 * <tt>public void AddEntry(int nodeKey, String ip, int port)</tt>
	 * <p>
	 * calls {@link FingerTable.AddDetails()} and {@link FingerTable.Update()}
	 * <p> Adds given details to the finger table.
	 * 
	 * @param  nodeKey value of the node key to be added to the finger table
	 * @param  ip string containing the ip of the node to be added
	 * @param  port value of the node port to be added
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
	 * <tt>public void AddDetails(int nodeKey, String ip, int port)</tt>
	 * <p>
	 * Adds given details to the allNodeDetails and allKeysInOrder fields and sorts allKeysInOrder. 
	 * 
	 * @param  nodeKey value of the node key to be added to the finger table
	 * @param  ip string containing the ip of the node to be added
	 * @param  port value of the node port to be added
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
	 * <tt>public String GetIp(int nodeKey)</tt>
	 * <p>
	 * Use this for searches not for file dist.
	 * <p>
	 * Returns the IP of the given node key if it is in start keys. OR .
	 * Returns the IP of the corresponding entry in the finger table to the given key in String format. 
	 * 
	 * @param  nodeKey value of the node key whose corresponding entry in finger table is required
	 * @return  IP of the node to send search req to.
	 */
	public String GetIp(int nodeKey) {
		return allNodeDetails.get(FindPrevKey(nodeKey)).get(0);
	}
	
	/**
	 * <tt>public String GetPort(int nodeKey)</tt>
	 * <p>
	 * Use this for searches not for file dist.
	 * <p>
	 * Returns the port of the given node key if it is in start keys. OR .
	 * Returns the port value of the corresponding entry in the finger table to the given key. 
	 * 
	 * @param  nodeKey value of the node key whose corresponding entry in finger table is required
	 * @return  port value of the node to send search req to.
	 */
	public int GetPort(int nodeKey) {
		return Integer.parseInt(allNodeDetails.get(FindPrevKey(nodeKey)).get(1));
	}
	
	/**
	 * <tt>public String GetIp(int nodeKey)</tt>
	 * <p>
	 * Use this for searches not for file dist.
	 * <p>
	 * Returns the IP of the given key in String format irrespective of its presence in finger table. 
	 * 
	 * @param  nodeKey value of the node key whose corresponding entry in finger table is required
	 * @return  IP of the node to send search req to.
	 */
	public String GetIpOfThis(int nodeKey) {
		return allNodeDetails.get(nodeKey).get(0);
	}
	
	/**
	 * <tt>public String GetPort(int nodeKey)</tt>
	 * <p>
	 * Use this for searches not for file dist.
	 * <p>
	 * Returns the port value of the given key irrespective of its presence in finger table. 
	 * 
	 * @param  nodeKey value of the node key whose corresponding entry in finger table is required
	 * @return  port value of the node to send search req to.
	 */
	public int GetPortOfThis(int nodeKey) {
		return Integer.parseInt(allNodeDetails.get(nodeKey).get(1));
	}

	/**
	 * <tt>public int GetSuccessor(int key)</tt>
	 * <p>
	 * Returns the key value of the successor entry to the given key in the finger table. 
	 * 
	 * @param  key value of the node key whose successor entry in finger table is required
	 * @return  key value of the successor node.
	 */
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
	
	/**
	 * <tt>public int GetPredecessor(int key)</tt>
	 * <p>
	 * Returns the key value of the predecessor entry to the given key in the finger table. 
	 * 
	 * @param  key value of the node key whose predecessor entry in finger table is required
	 * @return  key value of the predecessor node.
	 */
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
	
	/**
	 * <tt>public void RemoveEntry(int key)</tt>
	 * <p>
	 * Removes the entry in the finger table of the given key. 
	 * 
	 * @param  nodekey value of the node key who needds to be removed from the finger table.
	 */
	public void RemoveEntry(int key) {
		System.out.println("removing entry from allkeysinorder");
		allKeysInOrder.remove(new Integer(key));
		System.out.println("removing entry from allNodeDetails");
		allNodeDetails.remove(key);
		System.out.println("updating fingertable");
		Update();
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
	
	public void PrintStats() {
		System.out.println(allKeysInOrder+"\n");
		for(int i : allNodeDetails.keySet())
			System.out.println(i+": "+allNodeDetails.get(i));
		System.out.println("\n");
		Print();
		System.out.println("======");
	}
}
