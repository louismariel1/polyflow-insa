package customdatatypes;

public class Cat extends Animal {

    public Cat(float weight, String specie, String status){
        super(weight, specie, status);
    }
    @Override
    public String getName() {
        return "Cat";
    }
    @Override
    public String toString(){
        return "name: Cat, status: "+ getStatus() + ", specie: "+ getSpecie() + ", weight: "+ getWeight();
    }
}
