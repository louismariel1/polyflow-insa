package customdatatypes;

public class Rabbit extends Animal {

    public Rabbit(float weight, String specie, String status){
        super(weight, specie, status);
    }
    @Override
    public String getName() {
        return "Rabbit";
    }
    @Override
    public String toString(){
        return "name: Rabbit, status: "+ getStatus() + ", specie: "+ getSpecie() + ", weight: "+ getWeight();
    }
}
