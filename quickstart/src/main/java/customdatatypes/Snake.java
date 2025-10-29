package customdatatypes;

public class Snake extends Animal {

    public Snake(float weight, String specie, String status){
        super(weight, specie, status);
    }
    @Override
    public String getName() {
        return "Snake";
    }
    @Override
    public String toString(){
        return "name: Snake, status: "+ getStatus() + ", specie: " + getSpecie() + ", weight: "+ getWeight();
    }
}
