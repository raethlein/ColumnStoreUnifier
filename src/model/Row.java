package model;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Row {
	private Map<String, Attribute> attributes;
	private Key key;
	
	public Row(List<Attribute> attributes) {
		this.attributes = new HashMap<>();
		for(Attribute attribute : attributes){
			this.attributes.put(attribute.getName(), attribute);
		}
	}
	
	public Row(Key key, List<Attribute> attributes){
		this.key = key;
		this.attributes = new HashMap<>();
		for(Attribute attribute : attributes){
			this.attributes.put(attribute.getName(), attribute);
		}
	}

	public Collection<Attribute> getAttributes() {
		return attributes.values();
	}
	
	public Map<String, Attribute> getAttributesMap() {
		return attributes;
	}

	public void setAttributes(List<Attribute> attributes) {
		for(Attribute attribute : attributes){
			this.attributes.put(attribute.getName(), attribute);
		}
	}
	
	public void addAttribute(Attribute attribute){
		attributes.put(attribute.getName(), attribute);
	}

	public Key getKey() {
		return key;
	}

	public void setKey(Key key) {
		this.key = key;
	}
}
