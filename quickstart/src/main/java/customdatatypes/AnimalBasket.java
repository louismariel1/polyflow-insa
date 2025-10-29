package customdatatypes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/*
 * This class is our 'R' data type, which is algebraically a monoid:
 *  - The identity element is the Empty Basket
 *  - The associative binary operation is the union of two fruit baskets
 */
public class AnimalBasket implements Iterable<Animal>{

    private List<Animal> animals = new ArrayList<>();


    public void addAnimal(Animal a){
        this.animals.add(a);
    }

    public void addAll(AnimalBasket basket){
        basket.forEach(a->animals.add(a));
    }

    public int getSize(){
        return animals.size();
    }

    @Override
    public Iterator<Animal> iterator() {
        return animals.iterator();
    }
}
