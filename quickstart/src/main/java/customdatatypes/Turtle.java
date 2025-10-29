package customdatatypes;

public class Turtle extends Animal {

    public Turtle(float weight, String specie, String status){
        super(weight, specie, status);
    }
    @Override
    public String getName() {
        return "Turtle";
    }
    @Override
    public String toString(){
        return "name: Turtle, status: "+ getStatus() + ", specie: "+ getSpecie() + ", weight: "+ getWeight();
    }
}
