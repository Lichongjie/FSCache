package alluxio.client.file.cache.struct;

/**
 * Java 语言: 红黑树
 *
 * @author skywang
 * @date 2013/11/07
 * @editor KnIfER
 * @date 2017/11/18
 */

public class RBTree<T extends LinkNode<T>> {

  public LinkNode<T> mRoot;    // 根结点

  public RBTree() {
    mRoot = null;
  }

  private void print(LinkNode<T> tree, T key, int direction) {
    if (tree != null) {

      if (direction == 0)    // tree是根节点
        System.out.printf("%s(B) is root\n", tree.toString());
      else                // tree是分支节点
        System.out.printf("%s(%s) is %s's %6s child\n", tree.toString(),
                isRed(tree)
                        ? "R" : "B", key, direction == 1 ? "right" : "left");
      print(tree.left, (T) tree, -1);
      print(tree.right, (T) tree, 1);
    }
  }

  public void print() {
    System.out.println("print!!!!!!!!!!!!!!!");
    if (mRoot != null)
      print(mRoot, (T) mRoot, 0);
  }

  private static final boolean RED = false;
  private static final boolean BLACK = true;

  private LinkNode<T> parentOf(LinkNode<T> node) {
    return node != null ? node.parent : null;
  }

  private boolean colorOf(LinkNode<T> node) {
    return node != null ? node.color : BLACK;
  }

  private boolean isRed(LinkNode<T> node) {
    return ((node != null) && (node.color == RED)) ? true : false;
  }

  private boolean isBlack(LinkNode<T> node) {
    return !isRed(node);
  }

  private void setBlack(LinkNode<T> node) {
    if (node != null)
      node.color = BLACK;
  }

  private void setRed(LinkNode<T> node) {
    if (node != null)
      node.color = RED;
  }

  private void setParent(LinkNode<T> node, LinkNode<T> parent) {
    if (node != null)
      node.parent = (T) parent;
  }

  private void setColor(LinkNode<T> node, boolean color) {
    if (node != null)
      node.color = color;
  }


  private void leftRotate(LinkNode<T> x) {
// 设置x的右孩子为y
    LinkNode<T> y = x.right;

// 将 “y的左孩子” 设为 “x的右孩子”；
// 如果y的左孩子非空，将 “x” 设为 “y的左孩子的父亲”
    x.right = y.left;
    if (y.left != null)
      y.left.parent = (T) x;

// 将 “x的父亲” 设为 “y的父亲”
    y.parent = x.parent;

    if (x.parent == null) {
      this.mRoot = y;            // 如果 “x的父亲” 是空节点，则将y设为根节点
    } else {
      if (x.parent.left == x)
        x.parent.left = (T) y;    // 如果 x是它父节点的左孩子，则将y设为“x的父节点的左孩子”
      else
        x.parent.right = (T) y;    // 如果 x是它父节点的左孩子，则将y设为“x的父节点的左孩子”
    }

// 将 “x” 设为 “y的左孩子”
    y.left = (T) x;
// 将 “x的父节点” 设为 “y”
    x.parent = (T) y;
  }

  private void rightRotate(LinkNode<T> y) {
// 设置x是当前节点的左孩子。
    LinkNode<T> x = y.left;

// 将 “x的右孩子” 设为 “y的左孩子”；
// 如果"x的右孩子"不为空的话，将 “y” 设为 “x的右孩子的父亲”
    y.left = x.right;
    if (x.right != null)
      x.right.parent = (T) y;

// 将 “y的父亲” 设为 “x的父亲”
    x.parent = y.parent;

    if (y.parent == null) {
      this.mRoot = x;            // 如果 “y的父亲” 是空节点，则将x设为根节点
    } else {
      if (y == y.parent.right)
        y.parent.right = (T) x;    // 如果 y是它父节点的右孩子，则将x设为“y的父节点的右孩子”
      else
        y.parent.left = (T) x;    // (y是它父节点的左孩子) 将x设为“x的父节点的左孩子”
    }

// 将 “y” 设为 “x的右孩子”
    x.right = (T) y;

// 将 “y的父节点” 设为 “x”
    y.parent = (T) x;
  }


  private void insertFixUp(LinkNode<T> node) {
    LinkNode<T> parent, gparent;

// 若“父节点存在，并且父节点的颜色是红色”
    while (((parent = parentOf(node)) != null) && isRed(parent)) {
      gparent = parentOf(parent);

//若“父节点”是“祖父节点的左孩子”
      if (parent == gparent.left) {
// Case 1条件：叔叔节点是红色
        LinkNode<T> uncle = gparent.right;
        if ((uncle != null) && isRed(uncle)) {
          setBlack(uncle);
          setBlack(parent);
          setRed(gparent);
          node = gparent;
          continue;
        }

// Case 2条件：叔叔是黑色，且当前节点是右孩子
        if (parent.right == node) {
          LinkNode<T> tmp;
          leftRotate(parent);
          tmp = parent;
          parent = node;
          node = tmp;
        }

// Case 3条件：叔叔是黑色，且当前节点是左孩子。
        setBlack(parent);
        setRed(gparent);
        rightRotate(gparent);
      } else {    //若“z的父节点”是“z的祖父节点的右孩子”
// Case 1条件：叔叔节点是红色
        LinkNode<T> uncle = gparent.left;
        if ((uncle != null) && isRed(uncle)) {
          setBlack(uncle);
          setBlack(parent);
          setRed(gparent);
          node = gparent;
          continue;
        }

// Case 2条件：叔叔是黑色，且当前节点是左孩子
        if (parent.left == node) {
          LinkNode<T> tmp;
          rightRotate(parent);
          tmp = parent;
          parent = node;
          node = tmp;
        }

// Case 3条件：叔叔是黑色，且当前节点是右孩子。
        setBlack(parent);
        setRed(gparent);
        leftRotate(gparent);
      }
    }

// 将根节点设为黑色
    setBlack(this.mRoot);
  }


  public void insert(LinkNode<T> node) {
    int cmp;
    LinkNode<T> y = null;
    LinkNode<T> x = this.mRoot;

// 1. 将红黑树当作一颗二叉查找树，将节点添加到二叉查找树中。
    while (x != null) {
      y = x;
//System.out.println("add test");
      cmp = node.compareTo((T) x);
//System.out.println("add test finish");

      if (cmp < 0)
        x = x.left;
      else
        x = x.right;
    }

    node.parent = (T) y;
    if (y != null) {
      cmp = node.compareTo((T) y);
      if (cmp < 0)
        y.left = (T) node;
      else
        y.right = (T) node;
    } else {
      this.mRoot = node;
    }

// 2. 设置节点的颜色为红色
    node.color = RED;

// 3. 将它重新修正为一颗二叉查找树
    insertFixUp(node);
  }


  private void removeFixUp(LinkNode<T> node, LinkNode<T> parent) {
    LinkNode<T> other;

    while ((node == null || isBlack(node)) && (node != this.mRoot)) {
      if (parent.left == node) {
        other = parent.right;
        if (isRed(other)) {
// Case 1: x的兄弟w是红色的
          setBlack(other);
          setRed(parent);
          leftRotate(parent);
          other = parent.right;
        }

        if ((other.left == null || isBlack(other.left)) &&
                (other.right == null || isBlack(other.right))) {
// Case 2: x的兄弟w是黑色，且w的俩个孩子也都是黑色的
          setRed(other);
          node = parent;
          parent = parentOf(node);
        } else {

          if (other.right == null || isBlack(other.right)) {
// Case 3: x的兄弟w是黑色的，并且w的左孩子是红色，右孩子为黑色。
            setBlack(other.left);
            setRed(other);
            rightRotate(other);
            other = parent.right;
          }
// Case 4: x的兄弟w是黑色的；并且w的右孩子是红色的，左孩子任意颜色。
          setColor(other, colorOf(parent));
          setBlack(parent);
          setBlack(other.right);
          leftRotate(parent);
          node = this.mRoot;
          break;
        }
      } else {

        other = parent.left;
        if (isRed(other)) {
// Case 1: x的兄弟w是红色的
          setBlack(other);
          setRed(parent);
          rightRotate(parent);
          other = parent.left;
        }

        if ((other.left == null || isBlack(other.left)) &&
                (other.right == null || isBlack(other.right))) {
// Case 2: x的兄弟w是黑色，且w的俩个孩子也都是黑色的
          setRed(other);
          node = parent;
          parent = parentOf(node);
        } else {

          if (other.left == null || isBlack(other.left)) {
// Case 3: x的兄弟w是黑色的，并且w的左孩子是红色，右孩子为黑色。
            setBlack(other.right);
            setRed(other);
            leftRotate(other);
            other = parent.left;
          }

// Case 4: x的兄弟w是黑色的；并且w的右孩子是红色的，左孩子任意颜色。
          setColor(other, colorOf(parent));
          setBlack(parent);
          setBlack(other.left);
          rightRotate(parent);
          node = this.mRoot;
          break;
        }
      }
    }

    if (node != null)
      setBlack(node);
  }


  public void remove(LinkNode<T> node) {
    LinkNode<T> child, parent;
    boolean color;

// 被删除节点的"左右孩子都不为空"的情况。
    if ((node.left != null) && (node.right != null)) {
// 被删节点的后继节点。(称为"取代节点")
// 用它来取代"被删节点"的位置，然后再将"被删节点"去掉。
      LinkNode<T> replace = node;

// 获取后继节点
      replace = replace.right;
      while (replace.left != null)
        replace = replace.left;

// "node节点"不是根节点(只有根节点不存在父节点)
      if (parentOf(node) != null) {
        if (parentOf(node).left == node)
          parentOf(node).left = (T) replace;
        else
          parentOf(node).right = (T) replace;
      } else {
// "node节点"是根节点，更新根节点。
        this.mRoot = replace;
      }

// child是"取代节点"的右孩子，也是需要"调整的节点"。
// "取代节点"肯定不存在左孩子！因为它是一个后继节点。
      child = replace.right;
      parent = parentOf(replace);
// 保存"取代节点"的颜色
      color = colorOf(replace);

// "被删除节点"是"它的后继节点的父节点"
      if (parent == node) {
        parent = replace;
      } else {
// child不为空
        if (child != null)
          setParent(child, parent);
        parent.left = (T) child;

        replace.right = node.right;
        setParent(node.right, replace);
      }

      replace.parent = node.parent;
      replace.color = node.color;
      replace.left = node.left;
      node.left.parent = (T) replace;

      if (color == BLACK)
        removeFixUp(child, parent);

      node = null;
      return;
    }

    if (node.left != null) {
      child = node.left;
    } else {
      child = node.right;
    }

    parent = node.parent;
// 保存"取代节点"的颜色
    color = node.color;

    if (child != null)
      child.parent = (T) parent;

// "node节点"不是根节点
    if (parent != null) {
      if (parent.left == node)
        parent.left = (T) child;
      else
        parent.right = (T) child;
    } else {
      this.mRoot = child;
    }

    if (color == BLACK)
      removeFixUp(child, parent);
    node = null;
  }
}
