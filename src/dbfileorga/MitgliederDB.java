package dbfileorga;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class MitgliederDB implements Iterable<Record>
{
	
	protected DBBlock db[] = new DBBlock[8];
	private final boolean ordered;
	
	
	public MitgliederDB(boolean ordered){
		
		this.ordered = ordered;
		initDB();
		insertMitgliederIntoDB(ordered);
		
	}
	public MitgliederDB(){
		this.ordered = false;
		initDB();
	}
	
	private void initDB() {
		for (int i = 0; i<db.length; ++i){
			db[i]= new DBBlock();
		}
		
	}

	private java.util.List<Record> toList()
	{
		// Gibt alle Records der Datenbank in ihrer aktuellen Reihenfolge zurück
		java.util.ArrayList<Record> list = new java.util.ArrayList<>();
		for(Record r : this) list.add(r);
		return list;
	}

	private void rebuild(java.util.List<Record> list)
	{
		//Baut die gesamte Datenbank aus der übergebenen Liste neu auf
		initDB();
		for(Record r : list) appendRecord(r);
	}

	private void insertMitgliederIntoDB(boolean ordered) {
		MitgliederTableAsArray mitglieder = new MitgliederTableAsArray();
		String mitgliederDatasets[];
		if (ordered){
			mitgliederDatasets = mitglieder.recordsOrdered;
		}else{
			mitgliederDatasets = mitglieder.records;
		}
		for (String currRecord : mitgliederDatasets ){
			appendRecord(new Record(currRecord));
		}	
	}

		
	protected int appendRecord(Record record){
		//search for block where the record should be appended
		int currBlock = getBlockNumOfRecord(getNumberOfRecords());
		int result = db[currBlock].insertRecordAtTheEnd(record);
		if (result != -1 ){ //insert was successful
			return result;
		}else if (currBlock < db.length) { // overflow => insert the record into the next block
			return db[currBlock+1].insertRecordAtTheEnd(record);
		}
		return -1;
	}
	

	@Override
	public String toString(){
		String result = new String();
		for (int i = 0; i< db.length ; ++i){
			result += "Block "+i+"\n";
			result += db[i].toString();
			result += "-------------------------------------------------------------------------------------\n";
		}
		return result;
	}
	
	/**
	 * Returns the number of Records in the Database
	 * @return number of records stored in the database
	 */
	public int getNumberOfRecords(){
		int result = 0;
		for (DBBlock currBlock: db){
			result += currBlock.getNumberOfRecords();
		}
		return result;
	}
	
	/**
	 * Returns the block number of the given record number 
	 * @param recNum the record number to search for
	 * @return the block number or -1 if record is not found
	 */
	public int getBlockNumOfRecord(int recNum){
		int recCounter = 0;
		for (int i = 0; i< db.length; ++i){
			if (recNum <= (recCounter+db[i].getNumberOfRecords())){
				return i ;
			}else{
				recCounter += db[i].getNumberOfRecords();
			}
		}
		return -1;
	}
		
	public DBBlock getBlock(int i){
		return db[i];
	}
	
	
	/**
	 * Returns the record matching the record number
	 * @param recNum the term to search for
	 * @return the record matching the search term
	 */
	public Record read(int recNum){

		//Gültigkeit der Eingabe prÜfen
		if( recNum < 1 || recNum > getNumberOfRecords()) return  null;
		
		//Bestimmen der Blocknummer des gesuchten Records
		int blockNum = getBlockNumOfRecord(recNum);
		if (blockNum == -1) return null;

	    //Bestimmen der Position des gesuchten Records im Block
		int prefix = 0;
		for (int i = 0; i < blockNum; i++) prefix += db[i].getNumberOfRecords();

	    int posInBlock = recNum - prefix;

		// Rückgabe des gesuchten Records
		return db[blockNum].getRecord(posInBlock);
	}
	
	/**
	 * Returns the number of the first record that matches the search term
	 * @param searchTerm the term to search for
	 * @return the number of the record in the DB -1 if not found
	 */
	public int findPos(String searchTerm){

		int pos = 1;

		for (Record r : this)
		{
			//Vergleich des ersten Attributes mit dem searchTerm
			String id = r.getAttribute(1);
			if (id.equals(searchTerm))
			{
				return pos; //Datensatz gefunden
			}
			pos++;
		}

		return -1; //Kein pasender Datensatz gefunden
	}
	
	/**
	 * Inserts the record into the file and returns the record number
	 * @param record
	 * @return the record number of the inserted record
	 */
	public int insert(Record record){

		//Unordered-Weg
		if (!ordered)
		{
			int newPos = getNumberOfRecords() + 1;
			appendRecord(record);
			return newPos;
		}

		//Sorted-Weg
		java.util.List<Record> list = toList();

		// Mitgliedsnummer des neuen Datensatzes bestimmen
		int newId = Integer.parseInt(record.getAttribute(1));

		//Position zum Einfügen des neuen Datensatzues bestimmen
		int idx = 0;
		while (idx < list.size())
		{
			int currId = Integer.parseInt(list.get(idx).getAttribute(1));
			if (newId < currId) break;
			idx++;
		}

		//Neuen Datensatz an gefundener Stelle einfügen
		list.add(idx, record);
		
		//Datenbank mit neuem Datensatz wieder aufbauen
		rebuild(list);

		return idx + 1;
	}
	
	/**
	 * Deletes the record specified 
	 * @param numRecord number of the record to be deleted
	 */
	public void delete(int numRecord){
		
		//Eingabe prüfen
		int total = getNumberOfRecords();
		if (numRecord < 1 || numRecord >  total) return;

		//Liste aller Datensätze erstellen
		java.util.List<Record> list = toList();

		//Löschen des gegebenen Datensatzes aus der Liste
		list.remove(numRecord - 1);

		//Neuaufbau der Datenbank
		rebuild(list);

	}
	
	/**
	 * Replaces the record at the specified position with the given one.
	 * @param numRecord the position of the old record in the db
	 * @param record the new record
	 * 
	 */
	public void modify(int numRecord, Record record){
		
		//Eingabe prüfen
		int total = getNumberOfRecords();
		if (numRecord < 1 || numRecord > total) return;

		//Liste aller Datensätze erstelln
		java.util.List<Record> list = toList();

		//Unordered-Weg
		if(!ordered)
		{
			list.set(numRecord - 1, record);
			rebuild(list);
			return;
		}

		//Sorted-Weg
		//Alten Eintrag entfernen
		list.remove(numRecord - 1);

		//Neue Mitgliedsnummer bestimmen
		int newId = Integer.parseInt(record.getAttribute(1));

		//Einfügeposition des neuen Datensatzes bestimmen
		int idx = 0;
		while (idx < list.size())
		{
			int currId = Integer.parseInt(list.get(idx).getAttribute(1));
			if (newId < currId) break;
			idx++;
		}

		list.add(idx, record);
		rebuild(list);
	}

	
	@Override
	public Iterator<Record> iterator() {
		return new DBIterator();
	}
 
	private class DBIterator implements Iterator<Record> {

		    private int currBlock = 0;
		    private Iterator<Record> currBlockIter= db[currBlock].iterator();
	 
	        public boolean hasNext() {
	            if (currBlockIter.hasNext()){
	                return true; 
	            } else if (currBlock < (db.length-1)) { //continue search in the next block
	                return db[currBlock+1].iterator().hasNext();
	            }else{ 
	                return false;
	            }
	        }
	 
	        public Record next() {	        	
	        	if (currBlockIter.hasNext()){
	        		return currBlockIter.next();
	        	}else if (currBlock < db.length){ //continue search in the next block
	        		currBlockIter= db[++currBlock].iterator();
	        		return currBlockIter.next();
	        	}else{
	        		throw new NoSuchElementException();
	        	}
	        }
	 
	        @Override
	        public void remove() {
	        	throw new UnsupportedOperationException();
	        }
	    } 
	 

}
