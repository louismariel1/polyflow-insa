package customdatatypes;

public class Dog extends Animal {

    public Dog(float weight, String specie, String status){
        super(weight, specie, status);
    }
    @Override
    public String getName() {
        return "Dog";
    }
    @Override
    public String toString(){
        return "name: Dog, status: "+ getStatus() + ", specie: "+ getSpecie() + ", weight: "+ getWeight();
    }
}

