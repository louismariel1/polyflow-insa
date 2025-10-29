package customoperators;

import shared.contentimpl.EmptyContent;
import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class CustomFilterContentFactory<I, W, R> implements ContentFactory<I, W, R> {


    //Function to convert an element of type I in an element of type W
    Function<I, W> f1;

    //Function to convert an element of type W in an element of type R
    Function<W, R> f2;

    //Function to sum two elements of type R in a single result element of the same type
    BiFunction<R, R, R> sumR;

    //Predicate used to filter the elements entering the window
    Predicate<I> filterCondition;

    //Element of type R that represents an empty content
    R emptyContent;

    public CustomFilterContentFactory(Function<I, W> f1, Function<W, R> f2, BiFunction<R, R, R> sumR, R emptyContent, Predicate<I> filterCondition){
        this.f1 = f1;
        this.f2 = f2;
        this.sumR = sumR;
        this.emptyContent = emptyContent;
        this.filterCondition = filterCondition;
    }


    @Override
    public Content<I, W, R> createEmpty() {
        return new EmptyContent<>(emptyContent);
    }

    @Override
    public Content<I, W, R> create() {
        return new CustomFilterContent<>(f1, f2, sumR, emptyContent, filterCondition);
    }
}
