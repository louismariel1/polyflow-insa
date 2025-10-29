package customdatatypes;

public class Ant extends Animal {

    public Ant(float weight, String specie, String status){
        super(weight, specie, status);
    }
    @Override
    public String getName() {
        return "Ant";
    }
    @Override
    public String toString(){
        return "name: Monkey, status: "+ getStatus() + ", specie: " + getSpecie() + ", weight: "+ getWeight();
    }
}