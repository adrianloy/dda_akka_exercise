package de.hpi.akka_tutorial;

public class Participant {

	
	private int id; 
	private String name;
	private String pwhash;
	private String dna;
	private String pw_clear;
	private Participant dna_match;
	
	
	public Participant(int id, String name, String pwhash, String dna) {
		super();
		this.id = id;
		this.name = name;
		this.pwhash = pwhash;
		this.dna = dna;
	}


	public int getId() {
		return id;
	}


	public void setId(int id) {
		this.id = id;
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public String getPwhash() {
		return pwhash;
	}


	public void setPwhash(String pwhash) {
		this.pwhash = pwhash;
	}


	public String getDna() {
		return dna;
	}


	public void setDna(String dna) {
		this.dna = dna;
	}


	public String getPw_clear() {
		return pw_clear;
	}


	public void setPw_clear(String pw_clear) {
		this.pw_clear = pw_clear;
	}


	public Participant getDna_match() {
		return dna_match;
	}


	public void setDna_match(Participant dna_match) {
		this.dna_match = dna_match;
	}
	
	
}
