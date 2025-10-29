package customdatatypes;

public class Monkey extends Animal {

    public Monkey(float weight, String specie, String status){
        super(weight, specie, status);
    }
    @Override
    public String getName() {
        return "Monkey";
    }
    @Override
    public String toString(){
        return "name: Monkey, status: "+ getStatus() + ", specie: " + getSpecie() + ", weight: "+ getWeight();
    }
}
