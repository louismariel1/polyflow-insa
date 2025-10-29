package customdatatypes;

public abstract class Animal {

    private float weight;
    private String status;
    private String specie;
   
   
    public Animal(float weight, String specie, String status){
        this.weight = weight;
        this.status = status;
        this.specie = specie;
    }
    
    public String getName() {
        return "Animal";
    }
    

    public String getStatus() {
        return status;
    }

    public float getWeight() {
        return weight;
    }
    public String getSpecie() {
        return specie;
    }
 
}