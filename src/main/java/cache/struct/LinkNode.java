/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package cache.struct;

public abstract class LinkNode<T extends LinkNode> {
   public T before = null;
  public T after = null;
  public T parent = null;
  public  T left = null;
  public T right = null;
   boolean isLast = false;
   boolean color;


  public void setLast() {
    isLast = true;
  }

  public abstract int compareTo(T node);

}
