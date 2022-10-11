package sjdb;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

public class Optimiser {
	private Estimator estimator = new Estimator();

	public Optimiser(Catalogue catalog) {
	}

	public Operator optimise(Operator plan) {
		ArrayList<Select> arrList = new ArrayList<Select>();
		ArrayList<Select> valueList = new ArrayList<Select>();
		ArrayList<Select> value = new ArrayList<Select>();
		ArrayList<Select> attrib = new ArrayList<Select>();
		ArrayList<Scan> R = new ArrayList<Scan>();      
		ArrayList<Operator> operator = new ArrayList<Operator>();
		ArrayList<Join> joinList = new ArrayList<Join>();
		ArrayList<Project> project = new ArrayList<Project>();
		ArrayList<Project> initialProject = new ArrayList<Project>();
		iterate(plan, arrList, valueList, R, joinList, initialProject);

		plan=select_down(plan, arrList, valueList, value, attrib, R, operator, joinList, project);
		System.out.print("\n");
		plan = reorder(plan);
		System.out.print("\n");
		if (plan instanceof Project ){
			plan = push_up(plan);
			System.out.print("\n");
			plan = push_down(plan);
		}
	
		return plan;
	}
	public Operator iterate(Operator operator, ArrayList<Select> arrList, ArrayList<Select> valueList, ArrayList<Scan> R,
			ArrayList<Join> joinList, ArrayList<Project> initialProject) {
		if (operator instanceof Scan) {
			R.add((Scan) operator);
			return operator;
		}
		if (operator instanceof Select) {
			Boolean right = ((Select) operator).getPredicate().equalsValue();
			if (right) {
				valueList.add(((Select) operator));
			} else {
				arrList.add(((Select) operator));
			}
		}
		if (operator instanceof Join) {
			joinList.add((Join) operator);
		}
		if (operator instanceof Project) {
			initialProject.add((Project) operator);
		}
		iterate(operator.getInputs().get(0), arrList, valueList, R, joinList, initialProject);
		if (operator.getInputs().size() == 2) {
			iterate(operator.getInputs().get(1), arrList, valueList, R, joinList, initialProject);
		}
		return operator;
	}
	public Operator select_down(Operator operator, ArrayList<Select> arrList, ArrayList<Select> valueList,
			ArrayList<Select> value, ArrayList<Select> attrib, ArrayList<Scan> R, ArrayList<Operator> operator,
			ArrayList<Join> joinList, ArrayList<Project> project) {
		if (valueList.size() == 0 && arrList.size() == 1 ) {
			operator.add(0,operator);
			operator.add(0, test(operator.get(0),R, arrList));
		} else if (valueList.size() == 1 && arrList.size() == 0 && R.size() == 1) {
			operator.add(operator);
		} else if (valueList.size() == 0 && arrList.size() == 0) {
			operator.add(operator);
		} else {
			int size = valueList.size();
			for (int k = 0; k < size; k++) {
				for (int i = 0; i < R.size(); i++) {
					for (int j = 0; j < valueList.size(); j++) {
						if (R.get(i).getRelation().getAttributes().contains(valueList.get(j).getPredicate().getLeftAttribute())) {
							Select se = new Select(R.get(i), valueList.get(j).getPredicate());
							estimator.visit(se);
							value.add(se);
							R.remove(i);
							valueList.remove(j);
						}
					}
				}
			}
			if (arrList.size() == 0) {
				ArrayList<Product> pd = new ArrayList<Product>();
				for (int i=0; i<value.size(); i++){
					if (value.size() > 1) {
						Product pc = new Product(value.get(i), value.get(i+1));
						estimator.visit(pc);
						pd.add(pc);
						value.remove(i);
						value.remove(i);
						for (int j =0; j<value.size(); j++){
							Product po = new Product(pd.get(0), value.get(j));
							estimator.visit(po);
							value.remove(j);
							pd.add(0, po);
						}
						for (int k=0; k<R.size(); k++){
							Product pu = new Product(pd.get(0), R.get(k));
							estimator.visit(pu);
							pd.add(0, pu);
						}
					} else {
						Product pro = new Product(value.get(i),R.get(0));
						estimator.visit(pro);
						pd.add(0, pro);
						for (int n=1; n<R.size(); n++) {
							Product prod = new Product(pd.get(0),R.get(n));
							estimator.visit(prod);
							pd.add(0, prod);
							R.remove(n);
							value.remove(i);
						}
					}
				}
				if (operator instanceof Project) {
					Project pe = new Project(pd.get(0), ((Project)operator).getAttributes());
					estimator.visit(pe);
					operator.add(0, pe);
				} else {
					operator.add(pd.get(0));
				}
				
			} else {
				while(1>0){
					int size1 = arrList.size();
					for (int l=0;l<size1;l++){ 
						for (int i = 0; i < arrList.size(); i++) {
							Attribute left = arrList.get(i).getPredicate().getLeftAttribute();
							Attribute right = arrList.get(i).getPredicate().getRightAttribute();
							Select s1 = null;
							s1 = arrList.get(i);
							for (int n = 0; n < value.size(); n++) {
								if (value.get(n).getOutput().getAttributes().contains(right)
										|| value.get(n).getOutput().getAttributes().contains(left)) {
									ArrayList<Select> new_value = new ArrayList<Select>();
									new_value.addAll(value);
									new_value.remove(n);
									for (int t = 0; t < new_value.size(); t++) {
										if (new_value.get(t).getOutput().getAttributes().contains(left)
												|| new_value.get(t).getOutput().getAttributes().contains(right)) {
											Attribute left2 = s1.getPredicate().getLeftAttribute();
											Attribute right2 = s1.getPredicate().getRightAttribute();

											if (value.get(n).getOutput().getAttributes().contains(left2)) {
												Join jn = new Join(value.get(n), new_value.get(t),s1.getPredicate());
												estimator.visit(jn);
												joinList.add(jn);
											} else {
												Predicate prd = new Predicate(right2, left2);
												Join jn = new Join(value.get(n), new_value.get(t), prd);
												estimator.visit(jn);
												joinList.add(jn);
											}
											new_value.remove(t);
											value = (ArrayList<Select>) new_value.clone();
											arrList.remove(i);
											break;
										}
									}
									if (joinList.size()>0)
										break;
								}
							}
							if (joinList.size()>0)
								break;
						}
						if (joinList.size()>0)
							break;
					}
					if (joinList.size()>0)
						break;
					
					for (int l = 0; l < size1; l++) {
					for (int i = 0; i < arrList.size(); i++) {
						Attribute left = arrList.get(i).getPredicate().getLeftAttribute();
						Attribute right = arrList.get(i).getPredicate().getRightAttribute();
						Select s1 = null;
						s1 = arrList.get(i);
						for (int j = 0; j < R.size(); j++) {
							if (R.get(j).getRelation().getAttributes().contains(right)
									|| R.get(j).getRelation().getAttributes().contains(left)) {
									for (int k = 0; k < value.size(); k++) {
										if (value.get(k).getOutput().getAttributes().contains(right)
												|| value.get(k).getOutput().getAttributes().contains(left)) {
											Attribute left2 = s1.getPredicate().getLeftAttribute();
											Attribute right2 = s1.getPredicate().getRightAttribute();
											Product pd = new Product(value.get(k), R.get(j));
											estimator.visit(pd);
											Select se = new Select(pd, s1.getPredicate());
											estimator.visit(se);
											if (value.get(k).getOutput().getAttributes().contains(left2)) {
												Join jn = new Join(value.get(k), R.get(j), s1.getPredicate());
												estimator.visit(jn);
												joinList.add(jn);
											} else {
												Predicate prd = new Predicate(right2, left2);
												Join jn = new Join(value.get(k), R.get(j), prd);
												estimator.visit(jn);
												joinList.add(jn);
											}
											attrib.add(se);
											R.remove(j);
											value.remove(k);
											arrList.remove(i);
											break;
										}
									}
									if (joinList.size()>0)
										break;			
							}
						}
			
						if (joinList.size()>0)
							break;
					}
					if (joinList.size()>0)
						break;
				}
					if (joinList.size()>0)
						break;
					
					for (int l = 0; l < size1; l++) {
						for (int i = 0; i < arrList.size(); i++) {
							Attribute left = arrList.get(i).getPredicate().getLeftAttribute();
							Attribute right = arrList.get(i).getPredicate().getRightAttribute();
							Select s1 = null;
							s1 = arrList.get(i);
							ArrayList<Operator> cop_value = new ArrayList<Operator>();
							cop_value.addAll(value);
							for (int j = 0; j < R.size(); j++) {
								if (R.get(j).getRelation().getAttributes().contains(right)
										|| R.get(j).getRelation().getAttributes().contains(left)) {
										ArrayList<Scan> R3 = new ArrayList<Scan>();
										R3.addAll(R);
										R3.remove(j);
										for (int m = 0; m < R3.size(); m++) {
											if (R3.get(m).getRelation().getAttributes().contains(right)
													|| R3.get(m).getRelation().getAttributes().contains(left)) {
												Attribute left2 = s1.getPredicate().getLeftAttribute();
												Attribute right2 = s1.getPredicate().getRightAttribute();
	
												if (R3.get(m).getRelation().getAttributes().contains(left2)) {
													Join jn = new Join(R3.get(m), R.get(j), s1.getPredicate());
													estimator.visit(jn);
													joinList.add(jn);
												} else {
													Predicate prd = new Predicate(right2, left2);
													Join jn = new Join(R3.get(m), R.get(j), prd);
													estimator.visit(jn);
													joinList.add(jn);
												}
												R3.remove(m);
												R = (ArrayList<Scan>)R3.clone();
												arrList.remove(i);
												break;
											}
										}
										if (joinList.size()>0)
											break;
								}
							}
							
							if (joinList.size()>0)
								break;
						}
						if (joinList.size()>0)
							break;
					}
					if (joinList.size()>0)
						break;
				}

				if (arrList.size() == 0 && joinList.size() != 0 ) { // For the q4.txt
					ArrayList<Product> pd_ls = new ArrayList<Product>();
					ArrayList<Operator> operate = new ArrayList<Operator>();
					operate.add(0, joinList.get(0));
					if(value.size() != 0){
							Product pd = new Product(joinList.get(0), value.get(0));
							estimator.visit(pd);
							pd_ls.add(0, pd);
							int size2 = value.size();
							for (int k=0; k<size2; k++){
								for (int m=1; m<value.size(); m++){
									Product pd1 = new Product(pd_ls.get(0), value.get(m));
									estimator.visit(pd1);
									pd_ls.add(0, pd1);
									value.remove(m);
							}
						}
						operate.add(0, pd_ls.get(0));
					}
					if (R.size() != 0){
						int size3 = R.size();
						for (int p=0; p<size3; p++){
							for (int t=0; t<R.size();t++){
								Product pd2 = new Product(operate.get(0),R.get(t));
								estimator.visit(pd2);
								pd_ls.add(0, pd2);
								R.remove(t);
							}
						}
						operate.add(0, pd_ls.get(0));
					}
					if(plan instanceof Project){
						Project po = new Project(operate.get(0), ((Project) plan).getAttributes()); 	
						estimator.visit(po);
						operator.add(po);
					} else {
						operator.add(operate.get(0));
					}
					
				} else {
					int size2 = arrList.size();
					for (int i = 0; i < size2; i++) {
						for (int j = 0; j < arrList.size(); j++) {
							Attribute left = arrList.get(j).getPredicate().getLeftAttribute();
							Attribute right = arrList.get(j).getPredicate().getRightAttribute();
							Join jn = null;
								if (joinList.get(0).getOutput().getAttributes().contains(left)
										|| joinList.get(0).getOutput().getAttributes().contains(right)) {
									for (int l = 0; l < R.size(); l++) {
										if (R.get(l).getRelation().getAttributes().contains(right)
												|| R.get(l).getRelation().getAttributes().contains(left)) {
											if (joinList.get(0).getOutput().getAttributes().contains(left)) {
												jn = new Join(joinList.get(0), R.get(l), arrList.get(j).getPredicate());
												estimator.visit(jn);
												joinList.add(0,jn);
												R.remove(l);
												arrList.remove(j);	
											} else {
												Predicate prd = new Predicate(right, left);
												jn = new Join(joinList.get(0), R.get(l), prd);
												estimator.visit(jn);
												joinList.add(0,jn);
												R.remove(l);
												arrList.remove(j);	
											}
										}
									}
									for (int m = 0; m < value.size(); m++) {
										if (value.get(m).getOutput().getAttributes().contains(right)
												|| value.get(m).getOutput().getAttributes().contains(left)) {
											if (joinList.get(0).getOutput().getAttributes().contains(left)) {
												jn = new Join(joinList.get(0), value.get(m), arrList.get(j).getPredicate());
												estimator.visit(jn);
												joinList.add(0,jn);
												value.remove(m);
												arrList.remove(j);	
											} else {
												Predicate prd = new Predicate(right, left);
												jn = new Join(joinList.get(0), value.get(m), prd);
												estimator.visit(jn);
												joinList.add(0,jn);
												value.remove(m);
												arrList.remove(j);	
											}

										
										}
									}
								}
											
						}
				
						
					}
					if (R.size()!=0 || value.size() != 0){
						int size3 = R.size();
						int size4 = value.size();
						ArrayList<Operator> pd_ls = new ArrayList<Operator>();
						pd_ls.add(0, joinList.get(0));
						for (int q=0;q<size4; q++) {
							for (int t=0; t<value.size();t++){
								Product pd2 = new Product(pd_ls.get(0),value.get(t));
								estimator.visit(pd2);
								pd_ls.add(0, pd2);
								value.remove(t);
							}
						}
						for (int p=0; p<size3; p++){
							for (int t=0; t<R.size();t++){
								Product pd2 = new Product(pd_ls.get(0),R.get(t));
								estimator.visit(pd2);
								pd_ls.add(0, pd2);
								R.remove(t);
							}
						}
						if (plan instanceof Project){
							Project pj = new Project(pd_ls.get(0), ((Project) plan).getAttributes()); 
							estimator.visit(pj);
							operator.add(0,pj);

						} else {
							operator.add(0,pd_ls.get(0));

						}	
					} else {
						if (plan instanceof Project){
							Project pj = new Project(joinList.get(0), ((Project) plan).getAttributes()); 
							estimator.visit(pj);
							operator.add(0,pj);

						} else {
							operator.add(0,joinList.get(0));

						}	
					}
				}
			}
			
		}
		return operator.get(0);
	}


	public Operator test(Operator operator, ArrayList<Scan> R, ArrayList<Select> arrList) { 
		if (operator instanceof Project){
			crea_join(operator, R, arrList);
		}
		if (operator instanceof Select) {
			operator = crea(operator, R, arrList);
		}
		return operator;
	}
	
	public Operator crea_join(Operator operator, ArrayList<Scan> R, ArrayList<Select> arrList) { // for select
		Attribute left = arrList.get(0).getPredicate().getLeftAttribute();
		Attribute right = arrList.get(0).getPredicate().getRightAttribute();
		Scan left_re = null;
		Scan right_re = null;
		if (operator.getInputs().get(0) instanceof Select) {
			if (operator.getInputs().get(0).getInputs().get(0) instanceof Product){
				for (int i=0; i<R.size(); i++){
					if (R.get(i).getRelation().getAttributes().contains(left)) {
						left_re = R.get(i);
						R.remove(i);
					}
					if (R.get(i).getRelation().getAttributes().contains(right)) {
						right_re = R.get(i);
						R.remove(i);
					}		
				}
				Join jn = new Join(left_re, right_re, ((Select) operator.getInputs().get(0)).getPredicate());
				jn = reattr(jn);
				if (R.size() !=0) {
					ArrayList<Product> prd = new ArrayList<Product>();
					Product pd = new Product(jn, R.get(0));
					estimator.visit(pd);
					prd.add(0, pd);
					R.remove(0);
					if (R.size() !=0 ) {
						for (int j=0; j<R.size(); j++){
							Product pdt = new Product(prd.get(0), R.get(j));
							estimator.visit(pdt);
							prd.add(0, pdt);
							R.remove(j);	
						}
						operator.inputs.set(0, prd.get(0));
						return operator;
					} else {
						operator.inputs.set(0, prd.get(0));
						return operator;
					}			
				} else {
					operator.inputs.set(0, jn);
					return operator;
				}
			}
		}
		operator.inputs.set(0, crea_join(operator.getInputs().get(0),R, arrList));
		return operator;
	}
	public Operator crea(Operator operator, ArrayList<Scan> R, ArrayList<Select> arrList){ 		
		Attribute left = arrList.get(0).getPredicate().getLeftAttribute();
		Attribute right = arrList.get(0).getPredicate().getRightAttribute();
		Scan left_re = null;
		Scan right_re = null;
		if (operator.getInputs().get(0) instanceof Product) {
				for (int i=0; i<R.size(); i++){
					if (R.get(i).getRelation().getAttributes().contains(left)) {
						left_re = R.get(i);
						R.remove(i);
					}
					if (R.get(i).getRelation().getAttributes().contains(right)) {
						right_re = R.get(i);
						R.remove(i);
					}		
				}
				Join jn = new Join(left_re, right_re, ((Select) operator).getPredicate());
				jn = reattr(jn);
				if (R.size() !=0) {
					ArrayList<Product> prd = new ArrayList<Product>();
					Product pd = new Product(jn, R.get(0));
					estimator.visit(pd);
					prd.add(0, pd);
					R.remove(0);
					if (R.size() !=0 ) {
						for (int j=0; j<R.size(); j++){
							Product pdt = new Product(prd.get(0), R.get(j));
							estimator.visit(pdt);
							prd.add(0, pdt);
							R.remove(j);	
						}
						return prd.get(0);
					} else {
						return prd.get(0);
					}			
				} else {
					return jn;
				}
		}
		operator.inputs.set(0, crea(operator.getInputs().get(0),R, arrList));
		return operator;
	}


	public Operator reorder(Operator operator) {
		if (operator instanceof Scan) {	
			return operator;
		}
		operator.inputs.set(0, reorder(operator.getInputs().get(0)));
		if (operator instanceof Join)
			if (operator.getInputs().get(0) instanceof Join) {
				Attribute attr = ((Join)operator).getPredicate().getLeftAttribute();
				if (high(operator.getInputs().get(0).getInputs().get(0)) > high(operator.getInputs().get(1))){
					if(operator.getInputs().get(0).getInputs().get(0).getOutput().getAttributes().contains(attr)) {
						if (operator.getInputs().get(0).getInputs().get(1).getOutput().getTupleCount() > operator.getInputs().get(1).getOutput().getTupleCount()){
							Join second = null;
							second = new Join(operator.getInputs().get(0).getInputs().get(0), operator.getInputs().get(1), ((Join) operator).getPredicate());
							second = reattr(second);			
							Join first = new Join(second,operator.getInputs().get(0).getInputs().get(1),((Join)operator.getInputs().get(0)).getPredicate()) ;
							first = reattr(first);
							operator = first;
						}
					}
				} 
				else {
					if (operator.getInputs().get(0).getInputs().get(0).getOutput().getAttributes().contains(attr)){
						if (operator.getInputs().get(0).getInputs().get(1).getOutput().getTupleCount() > operator.getInputs().get(1).getOutput().getTupleCount()){
							Join second = null;
							second = new Join(operator.getInputs().get(0).getInputs().get(0), operator.getInputs().get(1), ((Join) operator).getPredicate());
							second = reattr(second);			
							Join first = new Join(second,operator.getInputs().get(0).getInputs().get(1),((Join)operator.getInputs().get(0)).getPredicate()) ;
							first = reattr(first);
							operator = first;
						}
					} else {
						if (operator.getInputs().get(0).getInputs().get(0).getOutput().getTupleCount() > operator.getInputs().get(1).getOutput().getTupleCount()){
							Join second = null;
							second = new Join(operator.getInputs().get(0).getInputs().get(1), operator.getInputs().get(1), ((Join) operator).getPredicate());
							second = reattr(second);			
							Join first = new Join(second,operator.getInputs().get(0).getInputs().get(0),((Join)operator.getInputs().get(0)).getPredicate()) ;
							first = reattr(first);
							operator = first;
						}
						
					}
				}
			}
		
		return operator;
	}
	
	public int high(Operator operator){
		int h=0;
		while (operator.getInputs() != null) {
			h++;
			operator = operator.getInputs().get(0);
		}
		return h;
	}
	
	public Join reattr(Join operator){
		Attribute left = operator.getPredicate().getLeftAttribute();
		Attribute right = operator.getPredicate().getRightAttribute();
		Join jn = null;
		if (operator.getInputs().get(0).getOutput().getAttributes().contains(left)) {
			jn = new Join(operator.getInputs().get(0), operator.getInputs().get(1), operator.getPredicate());
			estimator.visit(jn);
		} else {
			Predicate prd = new Predicate(right, left);
			jn = new Join(operator.getInputs().get(0), operator.getInputs().get(1), prd);
			estimator.visit(jn);
		}
		return jn;
	}

		public Operator push_up(Operator operator) {
			if (operator instanceof Scan) {
				return operator;
			}
			if (operator.getInputs().get(0) instanceof Product){
				ArrayList<Attribute> left_ls = new ArrayList<Attribute>();
				ArrayList<Attribute> right_ls = new ArrayList<Attribute>();
				
				for (int i=0; i < operator.getOutput().getAttributes().size(); i++){
					if(operator.getInputs().get(0).getInputs().get(0).getOutput().getAttributes().contains(operator.getOutput().getAttributes().get(i))){
						left_ls.add(operator.getOutput().getAttributes().get(i));
					} else {
						right_ls.add(operator.getOutput().getAttributes().get(i));
					}
				}
				if (left_ls.size() != 0) {
					Project prot_left = new Project(operator.getInputs().get(0).getInputs().get(0), left_ls);
					estimator.visit(prot_left);
					operator.getInputs().get(0).inputs.set(0, prot_left);
				}
				if (right_ls.size() !=0 ){
					Project prot_right = new Project(operator.getInputs().get(0).getInputs().get(1), right_ls);
					estimator.visit(prot_right);
					operator.getInputs().get(0).inputs.set(1, prot_right);
				}
				
			}
			if (operator instanceof Join) {
				ArrayList<Attribute> left_ls = new ArrayList<Attribute>();
				ArrayList<Attribute> right_ls = new ArrayList<Attribute>();
				Attribute left = ((Join) operator).getPredicate().getLeftAttribute();
				Attribute right = ((Join) operator).getPredicate().getRightAttribute();
				left_ls.add(left);
				right_ls.add(right);
				Project pro_left = new Project(operator.getInputs().get(0), left_ls);
				estimator.visit(pro_left);
				Project pro_right = new Project(operator.getInputs().get(1), right_ls);
				estimator.visit(pro_right);
				operator.inputs.set(0, pro_left);
				operator.inputs.set(1, pro_right);
			}
			operator.inputs.set(0, push_up(operator.getInputs().get(0)));
			return operator;
		}
		public Operator push_down(Operator operator){
			if (operator instanceof Scan) {
				return operator;
			}
			if (operator instanceof Project){
				if (operator.getInputs().get(0) instanceof Join) {
					List<Attribute> rs = operator.getInputs().get(0).getInputs().get(1).getOutput().getAttributes();
					List<Attribute> ls = operator.getInputs().get(0).getInputs().get(0).getOutput().getAttributes();
					for (int i=0; i<operator.getOutput().getAttributes().size(); i++){
						List<Attribute> left_ls = null;
						List<Attribute> right_ls = null;
						
						if (operator.getInputs().get(0).getInputs().get(0).getInputs().get(0).getOutput().getAttributes().contains(operator.getOutput().getAttributes().get(i))) {
							left_ls = operator.getInputs().get(0).getInputs().get(0).getOutput().getAttributes();
							if (!left_ls.contains(operator.getOutput().getAttributes().get(i))) {
								ls.add(operator.getOutput().getAttributes().get(i));
								
							}			
						} else {
							right_ls = operator.getInputs().get(0).getInputs().get(1).getOutput().getAttributes();
							if (!right_ls.contains(operator.getOutput().getAttributes().get(i))) {
								rs.add(operator.getOutput().getAttributes().get(i));				
							}	
						}
					}
					Project pro_left = new Project(operator.getInputs().get(0).getInputs().get(0).getInputs().get(0), ls);
					Project pro_right = new Project(operator.getInputs().get(0).getInputs().get(1).getInputs().get(0), rs);
					estimator.visit(pro_right);
					operator.inputs.get(0).inputs.set(1, pro_right);
					estimator.visit(pro_left);
					operator.inputs.get(0).inputs.set(0, pro_left);
				} 
			}
		
			operator.inputs.set(0, push_down(operator.getInputs().get(0)));
			return operator;
		}
	
}