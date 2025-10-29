package customoperators;

import org.streamreasoning.rsp4j.api.secret.content.Content;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/*
 * This is our custom content class, which represents how the element are stored in a window.
 * In this case, we also provided additional logic by filtering elements based on a predicate before adding them to the window.
 * This can be seen as an optimization: instead of performing the filtering inside the query (after having accumulated all the elements inside the window),
 * we perform it before the windowing operation.
 *
 * The content is highly customizable and should have an associated factory (The CustomFilterContentFactory can be found in the same directory),
 * but in order for the Content logic to work properly, a few functions should be defined:
 * 1- A function to transform an input element of type I to an element of type W. This is not mandatory (indeed, in our example we did not
 * perform such a transformation), but it can be a nice property to have in case we want to compress the elements we store.
 * 2- A function to transform an element of type W to an element of type R. The element of type R is the one on which the Relation To Relation operators
 * are defined (basically, all the transformations performed by queries take as input an element of type R and return another element of type R).
 * 3- A function to merge two elements of type R in a new element of type R. Each element of type W will be transformed in an element of type R and
 * "accumulated" or "merged" to return a single element of type R. For example, if the type W is a tuple and the type R is a table, each tuple will be
 * transformed in a table with a single row, and then merged together in a new table by concatenating the rows together.
 *
 * An empty content of type R should also be provided to act as the "neutral element" for the 'sumR' operation, and to have something to return when
 * the window is empty but used in a computation (for example, an Empty Table if we are in the Relational world).
 *
 * This example also defines a predicate, which is tested before adding an element to the content
 *
 *
 *
 */

public class CustomFilterContent<I, W, R> implements Content<I, W, R> {

    //Elements stored in the window, it is possible to define any type of data structure based on the need (Heap, Stack, Tree etc..)
    List<W> content = new ArrayList<>();

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


    public CustomFilterContent(Function<I, W> f1, Function<W, R> f2, BiFunction<R, R, R> sumR, R emptyContent, Predicate<I> filterCondition){
        this.f1 = f1;
        this.f2 = f2;
        this.sumR = sumR;
        this.emptyContent = emptyContent;
        this.filterCondition = filterCondition;
    }


    @Override
    public int size() {
        return content.size();
    }

    @Override
    public void add(I e) {
        if(filterCondition.test(e))
            content.add(f1.apply(e));
        else System.out.print("Not adding element "+e.toString()+" to window content\n");
    }

    @Override
    public R coalesce() {
        R result = content.stream().map(f2).reduce(emptyContent,  (x, y) -> sumR.apply(x,y));
        return result;
    }
}
