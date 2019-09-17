package TwoPhaseLocking;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.io.BufferedReader;

import java.io.FileNotFoundException;

  

 
public class MainClass {
	static Map<Integer,Transaction> transactionTableMap = new HashMap<Integer,Transaction>();
	static Map<String,Lock> lockTableMap = new HashMap<String,Lock>();

	public static void main(String[] args) {
		int timestamp=0;
		String filename="C:/Users/User/Desktop/DB2/Projectsummer2019/input7.txt";
		try{
			 FileReader fileReader = new FileReader(filename);		
			 BufferedReader bufferedReader = new BufferedReader(fileReader);
		
			String line1=null;
			 while ((line1=bufferedReader.readLine())!=null){
				 //operation = line.charAt(0);
				 String line = line1.replace(" ", "");
				 System.out.println(line);
				 if(line.charAt(0)=='b'){
					 timestamp=timestamp+1;
					begin(timestamp,Integer.parseInt(line.substring(1, line.indexOf(";"))));
				
				 System.out.println(Integer.parseInt(line.substring(1, line.indexOf(";"))));
				 }
       else if(line.charAt(0)=='r' || line.charAt(0)=='w'){
		  
    	  // System.out.println(line.substring(line.indexOf('(')+1,line.indexOf(')')));
		   //this function seperates the string into three parts . First part would check the name of the dataitem. Second part would check the transaction id and third would check the operation ie read 
    	   //or write and will pass all three parameters to request method. 
		 			 request(line.substring(line.indexOf('(')+1,line.indexOf(')')), Integer.parseInt(line.substring(1, line.indexOf('('))), line.charAt(0)+"");
				 }
       else if(line.charAt(0)=='e')
       {
    	   
    	 // System.out.println("Transaction" +Integer.parseInt(line.substring(1, line.indexOf(";")))+ " has released all the locks and is ready to commit" );   	
          releaseLock(Integer.parseInt(line.substring(1, line.indexOf(";"))));
       }			 
			 }
		}catch(Exception e){
			
			 System.out.println("Unable to open file '" +filename + "'");	
		}
		
	}
	 public static void begin(int timestamp,int transId){
		 Transaction trans= new Transaction( transId,timestamp,"Active");			
		 transactionTableMap.put(transId, trans);
		 System.out.println("Transaction "+transId+ " has begun and it has been entered in the transaction "
		 		+ "table with it's state as Active and timestamp as "+timestamp+".\n" );
	 }
	 public static void request(String dataItem, int transID, String op){
			op = op.equals("r") ? "Read" : "Write";
			Transaction t2 = transactionTableMap.get(transID); // Incoming ID
			//System.out.println(op); 
			//System.out.println(t2.state);
			if(t2.state=="Active")
				active(dataItem,t2,op);
			 //state of the transaction is active then	
			 //state of the transaction is blocked
			 else if(t2.state.equals("Block")){
				 block(dataItem, t2, op);
			 }
			 //state of the transaction is aborted
			 else if(t2.state.equals("Abort")){
				 System.out.println("Transaction "+t2.transId+" is aborted");			 
			 } 
			 //state of the transaction is committed
			 else if(t2.state.equals("Commit")){
				 System.out.println(" transaction "+t2.transId+" is committed");
			 }
		}
	 public static  void active(String dataItem, Transaction incomming, String op){
		
		 if(lockTableMap.containsKey(dataItem))
		 {
			 Lock lock = lockTableMap.get(dataItem);
		 if(lock.lockState.equals("Read") && op.equals("Read")){
			 lock = readread(dataItem, incomming, lock);
		 } 
		 else if(lock.lockState.equals("Read") && op.equals("Write")){
			 //System.out.println("I entered here");
			 lock = readwrite(dataItem, incomming, lock);
		 } 
		 else if(lock.lockState.equals("Write") && op.equals("Read")){
			 lock  = writeread(dataItem, incomming, lock);
		 } 
		 else if(lock.lockState.equals("Write") && op.equals("Write")){
			 lock = writewrite(dataItem, incomming, lock);
		 }
		 else if
		 (lock.lockState.equals("") && op.equals("Read")){
			 lock.lockState = "Read";
			 lock.readTransactionId.add(incomming.transId);
			 System.out.println("Transaction "+incomming.transId+" has acquired Read Lock on data item "+dataItem);
		 } else if(lock.lockState.equals("") && op.equals("Write")){
			 lock.lockState = "Write";
			 System.out.println("Transaction "+incomming.transId+" has acquired Write Lock on data item "+dataItem);
			 lock.writeLockTransId = incomming.transId;
		 }
		 lockTableMap.put(dataItem,lock);
			 
		 }
		 else{
		 Lock lock=null;
		 if(op.equals("Read")){
		 lock=new Lock(dataItem,op,0);
		 lock.readTransactionId.add(incomming.transId);
		 System.out.println("Transaction  "+incomming.transId+" is Active so entry for data item "+dataItem+" has been "
			 		+ "made in the lock table and transaction "+incomming.transId+" has acquired "
			 				+ "Read Lock on it."+"\n");
		 }
		 if(op.equals("Write"))
		 {
			 lock=new Lock(dataItem,op,incomming.transId);
			 System.out.println("Transaction "+incomming.transId+" is Active so entry for data item "+dataItem+" has been "
				 		+ "made in the lock table and transaction "+incomming.transId+" has acquired "
		 				+ "Write Lock on it."+"\n");
		 }
		 if(!incomming.DataItems.contains(dataItem))
		 {
			 incomming.DataItems.add(dataItem);
			// System.out.println(transactionTableMap.containsKey(incomming.transId));
		 transactionTableMap.put(incomming.transId, incomming);
		 lockTableMap.put(dataItem, lock);
		}
	 }
	 }	

	//lock state in lock table is read and the state of incoming transaction is also read
		public static Lock readread(String dataItem, Transaction in, Lock lock){
			 lock.readTransactionId.add(in.transId);
			 if(!in.DataItems.contains(dataItem))//If another transaction applies read lock on the same item
			 in.DataItems.add(dataItem);
			 transactionTableMap.put(in.transId, in);
			 System.out.println("Transaction "+in.transId+" has been appended in the read transaction id list for item "
			 		+dataItem+ ". That is, it has also acquired Read Lock on "+dataItem+".\n");
			 return lock;
		}
		public static Lock readwrite(String dataItem, Transaction in, Lock lock)
		{
			//System.out.println("Lock transaction Id"+lock.readTransactionId);
			
			//System.out.println("Incoming transaction Id "+in.transId);
			if(lock.readTransactionId.get(0).equals(in.transId)){// If the item is same and have same transaction Id  
				
				 lock.lockState="Write";
				 lock.readTransactionId.remove(0);//Removing  read transaction from read arraylist in lock table 
				 lock.writeLockTransId=in.transId;
				 System.out.println("For the data item "+dataItem+" and transaction ID "+in.transId+" lock has been upgraded to Write Lock."+"\n");
				 	}
			 else if(lock.readTransactionId.size()==1 && !lock.readTransactionId.get(0).equals(in.transId)){
				 Transaction t1 = transactionTableMap.get(lock.writeLockTransId);//
				 if(t1.timestamp>in.timestamp){
					 t1.state="Abort";
					 transactionTableMap.put(t1.transId, t1);
					 lock.lockState="Write";
					 lock.writeLockTransId=in.transId;
					 lock.readTransactionId.remove(0);
					 System.out.println("Transaction "+t1.transId+" abortes because of higher timestamp and transaction"
					 +in.transId+" acquires Write Lock on data item "+dataItem+".\n");
					 releaseitem(t1,dataItem);
					 
				 }
				 else{
					 in.state = "Block";
					 in.waitingOperation.add(new Operation("Write",dataItem));
					 transactionTableMap.put(in.transId,in);							 
					 lock.waitingList.add(in.transId);
					 System.out.println("Transaction "+in.transId+" is blocked because of higher timestamp and write "
					 		+ "operation for "+dataItem +" has been added to the waiting operation queue of transaction table and the transactio ID"
					 		+in.transId+" has been added to the waiting list queue of lock table."+"\n");
					 }
			 }
				 
			
			if(!in.DataItems.contains(dataItem))
				 in.DataItems.add(dataItem);
				 transactionTableMap.put(in.transId, in);
				
			return lock; 		
              }
		public static Lock writeread(String dataItem, Transaction in, Lock lock)
		{ 
			
			 if(lock.writeLockTransId == in.transId){
				 lock.lockState="Read";
				 lock.writeLockTransId=0;
				 lock.readTransactionId.add(in.transId);
				 if(!in.DataItems.contains(dataItem))
				 in.DataItems.add(dataItem);
				 transactionTableMap.put(in.transId, in);
				 System.out.println("For the data item "+dataItem+" and transaction ID "+in.transId+" lock has been downgraded to Read Lock."+"\n");
			 }
			 else
			 {
				 Transaction t1=transactionTableMap.get(lock.writeLockTransId);
				 if(t1.timestamp>in.timestamp){
					 t1.state="Abort";
					 transactionTableMap.put(t1.transId, t1);
					 lock.writeLockTransId=0;
					 lock.lockState="Read";
					 lock.readTransactionId.add(in.transId);	 
					 if(!in.DataItems.contains(dataItem))
							in.DataItems.add(dataItem);
					     transactionTableMap.put(in.transId, in);
						//System.out.println("For the data item "+dataItem+" and transaction ID "+in.transId+" lock has acquired to Read Lock."+"\n");
				 
						 System.out.println("Transaction "+t1.transId+" abortes because it has higher timestamp and transaction "
								 +in.transId+" acquires Read Lock on data item "+dataItem+"\n");
								 releaseitem(t1,dataItem);
				 
				 }
				 else
				 {
					 in.state="Block";
					 in.waitingOperation.add(new Operation("Read",dataItem));
					 
					 lock.waitingList.add(in.transId);
					 if(!in.DataItems.contains(dataItem))
						 in.DataItems.add(dataItem);
					 transactionTableMap.put(in.transId, in);
					 System.out.println("Transaction "+in.transId+" has been blocked and Read operation "
					 +dataItem+" has been added to the waiting operation queue in the transaction table and transaction Id "+in.transId+
					 " has been added to the waiting list queue in the lock table."+"\n");					 
				 }				 
			 }
			
			return lock;
		}
		public static void releaseLock(int  transactionid){
			Transaction t2=transactionTableMap.get(transactionid);
			if(t2.state.equals("Active")){
				t2.state ="Commit";
				Queue<String>  DataItems = t2.DataItems;
				
				while(!DataItems.isEmpty()){
					String d = DataItems.remove();
					releaseitem(t2,d);
					
			 }
				
				System.out.println("Releasing locks aquired by transaction "+transactionid);
					
			}
			else if(t2.state.equals("Block")){
				 t2.waitingOperation.add(new Operation("Commit", ""));
				 transactionTableMap.put(t2.transId, t2);	
				 System.out.println("Commit operation on transaction "+t2.transId+" has been added to the waiting operation");
			} 
			else if (t2.state.equals("Abort")){
				System.out.println("Transaction "+t2.transId+" cannot be committed because it has already been aborted.");
			}
			}
		
		public static void releaseitem(Transaction in, String dataItem){
			Lock lock = lockTableMap.get(dataItem);
			
			if(lock.lockState.equals("Write") || lock.readTransactionId.size()==1){
				Queue<Integer> wt= lock.waitingList;
				lock.lockState = "";
				if(lock.readTransactionId.size()==1){
					lock.readTransactionId.remove(0);
					System.out.println("Transaction "+in.transId+" has released read lock on "+dataItem);
				}else{
					System.out.println("Transaction "+in.transId+" has released write lock on "+dataItem);
				}
				lockTableMap.put(dataItem, lock);
				if(wt.isEmpty()){
					
					lockTableMap.remove(dataItem);					
				
				}
				else{
					while(!lock.waitingList.isEmpty()){
						
						
						int tid = lock.waitingList.remove();
						Transaction t = transactionTableMap.get(tid);					
						t = acquireLocks(t, dataItem, lock);
						transactionTableMap.put(tid, t);
						if(!t.state.equals("Commit")){
							return;
						}
					}
				}
				
				lockTableMap.remove(dataItem);	
			}
			else if(lock.lockState.equals("Read")){
				List<Integer> rtids = lock.readTransactionId;
				for(int i = 0; i < rtids.size(); ++i ){
					if(rtids.get(i) == in.transId){
						rtids.remove(i);
					}
				}	
				System.out.println("Transaction "+in.transId+" has released read lock on "+dataItem);
				lockTableMap.put(dataItem, lock);
			}
		
		}		
		public static void block(String dataItem, Transaction in, String op){
			 if(lockTableMap.containsKey(dataItem)){ 
				 Lock lock = lockTableMap.get(dataItem);	
				 lock.waitingList.add(in.transId);
				 lockTableMap.put(dataItem,lock); 
			 } 
			 if(!in.DataItems.contains(dataItem))
				 in.DataItems.add(dataItem);
			 in.waitingOperation.add(new Operation(op,dataItem));
			 transactionTableMap.put(in.transId,in); 
			 System.out.println("Transaction "+in.transId+" is in blocked state "+op+" operation on dataitem "
					 +dataItem+" has been added to the waiting operation queue of transaction table and the transactio ID"
					 		+in.transId+" has been added to the waiting list queue of lock table."+"\n");
		}

		
		public static Transaction acquireLocks(Transaction in, String dataItem, Lock lock){
			Queue<Operation> wo = in.waitingOperation;
			in.state="Active";//
			transactionTableMap.put(in.transId,in);
		//	System.out.println("I entered in acquired locks");
			if(!wo.isEmpty()){
				System.out.println("Transaction "+in.transId+" has been changed from Block to Active");
				System.out.println("Running its waiting operations");
			}
			while(!wo.isEmpty()){
				Operation o = wo.remove();
				if(o.operation.equals("Read")){
					request(o.dataItem, in.transId,"r");
				} else if(o.operation.equals("Write")){
					request(o.dataItem, in.transId,"w");
				} else if(o.operation.equals("Commit")){
					releaseLock(in.transId);
					
				}
			
				
			}
			
			lockTableMap.put(dataItem, lock);
			
			return in;
		}
public static Lock writewrite(String dataItem, Transaction in, Lock lock)
{
	
	 Transaction t1 = transactionTableMap.get(lock.writeLockTransId);
	 if(t1.timestamp>in.timestamp){
		 t1.state="Abort";
		 transactionTableMap.put(t1.transId, t1);
		 lock.writeLockTransId=in.transId;
		 System.out.println("Transaction "+t1.transId+" is aborted because of higher timestamp and transaction"
				 +in.transId+" has acquired write lock for data item"+dataItem);
		 releaseitem(t1,dataItem);
	 } 
	 else{
		 in.state = "Block";
		 transactionTableMap.put(in.transId,in);						 
		 lock.waitingList.add(in.transId);	
		 System.out.println("Transaction "+in.transId+" has been blocked because of high timestamp and write "
		 		+ "operation for "+ dataItem +" has been added to the waiting operation queue of transaction table "+"\n");
	 }
	 if(!in.DataItems.contains(dataItem)){
		 in.DataItems.add(dataItem);
		 transactionTableMap.put(in.transId, in);
	 }
	return lock;
}
}
