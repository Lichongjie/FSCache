package cache;

public interface PreviousIterator<E> {

  public boolean hasPrevious();

  public E previous();

  public E getBegin();
}
