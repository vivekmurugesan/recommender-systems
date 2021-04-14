package org.reco.sys.arm;

import java.io.Serializable;

public class Rule implements Serializable {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String antecedent;
	private String consequent;
	private double support;
	private double confidence;
	private double lift;
	private double antSupport;
	private double consSupport;
	
	
	public Rule(String antecedent, String consequent) {
		this.antecedent = antecedent;
		this.consequent = consequent;
	}
	public String getAntecedent() {
		return antecedent;
	}
	public void setAntecedent(String antecedent) {
		this.antecedent = antecedent;
	}
	public String getConsequent() {
		return consequent;
	}
	public void setConsequent(String consequent) {
		this.consequent = consequent;
	}
	public double getSupport() {
		return support;
	}
	public void setSupport(double support) {
		this.support = support;
	}
	public double getConfidence() {
		return confidence;
	}
	public void setConfidence(double confidence) {
		this.confidence = confidence;
	}
	public double getLift() {
		return lift;
	}
	public void setLift(double lift) {
		this.lift = lift;
	}
	public double getAntSupport() {
		return antSupport;
	}
	public void setAntSupport(double antSupport) {
		this.antSupport = antSupport;
	}
	public double getConsSupport() {
		return consSupport;
	}
	public void setConsSupport(double consSupport) {
		this.consSupport = consSupport;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((antecedent == null) ? 0 : antecedent.hashCode());
		result = prime * result + ((consequent == null) ? 0 : consequent.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Rule other = (Rule) obj;
		if (antecedent == null) {
			if (other.antecedent != null)
				return false;
		} else if (!antecedent.equals(other.antecedent))
			return false;
		if (consequent == null) {
			if (other.consequent != null)
				return false;
		} else if (!consequent.equals(other.consequent))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "Rule [antecedent=" + antecedent + ", consequent=" + consequent + ", antSupport=" + antSupport
				+ ", consSupport=" + consSupport + ", support=" + support
				+ ", confidence=" + confidence + ", lift=" + lift + "]";
	}
	
	public String toCsvString() {
		return antecedent + ", " + consequent + ", " + antSupport
				+ ", " + consSupport + ", " + support
				+ ", " + confidence + ", " + lift;
	}
	
	
}
