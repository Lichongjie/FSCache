package alluxio.client.file.cache.submodularLib;

import java.util.Set;

public abstract class IterateOptimizer<T extends Element> {

  public abstract void init();

  public void optimize() {
    init();
    while(!convergence()) {
      iterateOptimize();
    }
  }

  public abstract void iterateOptimize();

  public abstract boolean convergence();

  public abstract Set<T> getResult();
}
