package sjdb;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {


	public Estimator() {

	}
	
	public void visit(Scan scanVisit) {
		Relation input = scanVisit.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iterate = input.getAttributes().iterator();
		while (iterate.hasNext()) {
			output.addAttribute(new Attribute(iterate.next()));
		}
		
		scanVisit.setOutput(output);
	}

	public void visit(Project projectVisit) {
		Relation input = projectVisit.getInput().getOutput();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = projectVisit.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(input.getAttribute(iter.next())));
		}
		
		projectVisit.setOutput(output);
	}
	
	public void visit(Select selectVisit) {
		Relation input = op.getInput().getOutput();
		Relation output;
		Attribute left = null;
		Attribute right = null;
		int left_count;
		int right_count;
		int val_num;
		
		left = input.getAttribute(selectVisit.getPredicate().getLeftAttribute());
		left_count = left.getValueCount();
		
		if(selectVisit.getPredicate().equalsValue()) {
			output = new Relation(input.getTupleCount()/left_count);
			val_num = 1;
		} else {
			right = input.getAttribute(selectVisit.getPredicate().getRightAttribute());
			right_count = right.getValueCount();
			output = new Relation(input.getTupleCount()/Math.max(left_count, right_count));
			val_num = Math.min(left_count, right_count);
		}
		
		Iterator<Attribute> iterate = input.getAttributes().iterator();
		while (iterate.hasNext()) {
			Attribute attrib = iterate.next();																																																				
			if (attrib.equals(right)||attrib.equals(left)) {//
				output.addAttribute(new Attribute(attrib.getName(),val_num));
			} else {
				output.addAttribute(new Attribute(attrib));
			}
			
		}
		
		selectVisit.setOutput(output);
		
	}
	
	public void visit(Product productVisit) {
		Relation left = productVisit.getLeft().getOutput();
		Relation right = productVisit.getRight().getOutput();
		Relation output = new Relation(left.getTupleCount()*right.getTupleCount());
		
		Iterator<Attribute> iter_left = left.getAttributes().iterator();
		Iterator<Attribute> iter_right = right.getAttributes().iterator();
		while (iter_left.hasNext()) {
			output.addAttribute(new Attribute(iter_left.next()));
		}
		while (iter_right.hasNext()){
			output.addAttribute(new Attribute(iter_right.next()));
		}
		
		productVisit.setOutput(output);
	}
	
	public void visit(Join joinVisit) {
		Relation left = joinVisit.getLeft().getOutput();
		Relation right = joinVisit.getRight().getOutput();
		Predicate p = joinVisit.getPredicate();
        int left_count = left.getAttribute(p.getLeftAttribute()).getValueCount();
        int right_count = right.getAttribute(p.getRightAttribute()).getValueCount();
        int val_num = Math.min(left_count, right_count);
        
        Relation output = new Relation(left.getTupleCount()*right.getTupleCount()/Math.max(left_count, right_count));
		
        Iterator<Attribute> iter_left = left.getAttributes().iterator();
        Iterator<Attribute> iter_right = right.getAttributes().iterator();
        while (iter_left.hasNext()) {
        	Attribute atr_left = iter_left.next();
        	if (atr_left.equals(p.getLeftAttribute())) {
        		output.addAttribute(new Attribute(atr_left.getName(), val_num));
        	} else {
        		output.addAttribute(new Attribute(atr_left));
        	}
        }
        while (iter_right.hasNext()) {
        	Attribute atr_right = iter_right.next();
        	if (atr_right.equals(p.getRightAttribute())) {
        		output.addAttribute(new Attribute(atr_right.getName(), val_num));
        	} else {
        		output.addAttribute(new Attribute(atr_right));
        	}
        }
        
		joinVisit.setOutput(output);
	}
}
