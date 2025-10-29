package customoperators;

import customdatatypes.AnimalBasket;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;

import java.util.List;

public class JoinAnimalBasketOp implements RelationToRelationOperator<AnimalBasket> {
    List<String> tvgNames;
    String resName;

    public JoinAnimalBasketOp(List<String> tvgNames, String resName){
        this.tvgNames = tvgNames;
        this.resName = resName;
    }

    @Override
    public AnimalBasket eval(List<AnimalBasket> datasets) {
        AnimalBasket res = new AnimalBasket();
        res.addAll(datasets.get(0));
        res.addAll(datasets.get(1));
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
