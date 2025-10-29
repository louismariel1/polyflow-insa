package customoperators;

import customdatatypes.Animal;
import customdatatypes.AnimalBasket;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;

import java.util.List;

public class FilterAnimalByStatusOp implements RelationToRelationOperator<AnimalBasket> {

    // Name of the operands (one operand in this case)
    List<String> tvgNames;
    //Name of the result
    String resName;
    //Attribute to filter out
    String query;

    public FilterAnimalByStatusOp(String query, List<String> tvgNames, String resName){
        this.query = query;
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public AnimalBasket eval(List<AnimalBasket> datasets) {
        AnimalBasket op = datasets.get(0);
        AnimalBasket res = new AnimalBasket();
        //Add only the animals with a status different from the one passed to the query
        for(Animal animal : op){
            if(!animal.getStatus().equals(query))
                res.addAnimal(animal);
        }
        return res;
    }

    @Override
    public List<String> getTvgNames() {
        return tvgNames;
    }

    @Override
    public String getResName() {
        return resName;
    }
}

